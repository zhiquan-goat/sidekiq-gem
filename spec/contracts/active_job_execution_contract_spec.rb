require_relative '../support/active_job_runtime'

RSpec.describe SidekiqRuntime::ActiveJob::JobWrapper do
  include ActiveJobRuntimeSupport

  def retrying_job(&action)
    build_job do
      retry_on ArgumentError, wait: 10, attempts: 2, jitter: 0
      self.action = action || ->(*) { raise ArgumentError, 'business failure' }
    end
  end

  it 'schedules one AJ retry with stable logical identity and new provider JID, then succeeds' do
    child = build_job('SpecRuntimeAJChild')
    job_class = retrying_job do |*|
      raise ArgumentError, 'try again' if executions == 1
      SpecRuntimeAJChild.perform_later('child')
    end
    job = job_class.perform_later({id: 9, requested_at: 'original'})
    initial_args = persisted('ready').fetch(0).dig('args', 0, 'arguments')
    process_next
    successor = persisted('schedule').fetch(0)
    expect(successor['jid']).not_to eq(job.provider_job_id)
    expect(successor.dig('args', 0)).to include('job_id' => job.job_id, 'executions' => 1, 'arguments' => initial_args)
    expect(successor.dig('args', 0, SidekiqRuntime::JobMetadata::KEY, 'redelivered')).to be(true)
    expect(persisted('retry')).to be_empty
    expect(store.size).to eq(0)
    promote
    process_next
    process_next
    expect(job_class.records.map { |r| r[:context].redelivered? }).to eq([false, true])
    expect(job_class.records.map { |r| r[:jid] }).to eq([job.provider_job_id, successor['jid']])
    expect(job_class.records.map { |r| r[:executions] }).to eq([1, 2])
    expect(child.records.fetch(0)[:context].redelivered?).to be(false)
    expect(outcomes).to eq([:retry_scheduled, :succeeded, :succeeded])
    expect(persisted('dead') + persisted('retry') + persisted('schedule')).to be_empty
    expect(store.size).to eq(0)
  end

  it 'puts an unhandled exhausted retry in Dead without starting native retries' do
    job_class = retrying_job
    job_class.perform_later('exhaust')
    process_next
    promote
    expect { process_next }.to raise_error(ArgumentError, 'business failure')
    expect(job_class.records.size).to eq(2)
    expect(persisted('dead').size).to eq(1)
    expect(persisted('retry') + persisted('schedule')).to be_empty
    expect(store.size).to eq(0)
    expect(outcomes).to eq([:retry_scheduled, :failed])
  end

  it 'respects the retry exhaustion block without treating it as business success or Dead' do
    job_class = build_job do
      retry_on ArgumentError, wait: 10, attempts: 2, jitter: 0 do |job, error|
        job.class.records << {handled: error.message}
      end
      self.action = ->(*) { raise ArgumentError, 'handled exhaustion' }
    end
    job_class.perform_later
    process_next
    promote
    process_next
    expect(job_class.records.last).to eq(handled: 'handled exhaustion')
    expect(persisted('dead') + persisted('retry') + persisted('schedule')).to be_empty
    expect(outcomes).to eq([:retry_scheduled, :retry_exhausted])
  end

  it 'respects discard_on and arbitrary rescue_from handlers' do
    discarded = build_job('SpecAJDiscarded') do
      discard_on ArgumentError
      self.action = ->(*) { raise ArgumentError }
    end
    handled = build_job('SpecAJHandled') do
      rescue_from(ArgumentError) { |_error| nil }
      self.action = ->(*) { raise ArgumentError }
    end
    discarded.perform_later
    handled.perform_later
    process_next
    process_next
    expect(outcomes).to eq([:discarded, :handled])
    expect(persisted('dead')).to be_empty
    expect(store.size).to eq(0)
  end

  it 'does not interpret after_discard on an unhandled error as a discard decision' do
    job_class = build_job do
      after_discard { |job, _| job.class.records << {after_discard: true} }
      self.action = ->(*) { raise ArgumentError, 'unhandled' }
    end
    job_class.perform_later
    expect { process_next }.to raise_error(ArgumentError)
    expect(job_class.records.last).to eq(after_discard: true)
    expect(outcomes).to eq([:failed])
    expect(persisted('dead').size).to eq(1)
  end

  it 'distinguishes an aborted perform callback from successful business execution' do
    job_class = build_job { before_perform { throw(:abort) } }
    job_class.perform_later
    process_next
    expect(job_class.records).to be_empty
    expect(outcomes).to eq([:aborted])
    expect(store.size).to eq(0)
  end

  [nil, false].each do |refusal|
    it "retains a Dead failure when AJ swallows EnqueueError from middleware refusal #{refusal.inspect}" do
      job_class = retrying_job
      filter_retries(refusal)
      job = job_class.perform_later('retry filtered')
      expect { process_next }.to raise_error(SidekiqRuntime::ActiveJob::RetryPersistenceError)
      expect(persisted('schedule')).to be_empty
      expect(persisted('dead').fetch(0)).to include('jid' => job.provider_job_id,
        'error_class' => 'SidekiqRuntime::ActiveJob::RetryPersistenceError')
      expect(store.size).to eq(0)
      expect(outcomes).to eq([:retry_unconfirmed])
    end
  end

  it 'detects callback-cancelled retry despite a successfully enqueued child and another job with the same job_id' do
    child = build_job('SpecAJUnrelatedChild')
    job_class = build_job do
      before_enqueue { throw(:abort) if executions.positive? }
      self.action = lambda do |*|
        SpecAJUnrelatedChild.perform_later('child')
        other = self.class.new('same id, independent enqueue')
        other.job_id = job_id
        other.enqueue
        retry_job(wait: 10)
      end
    end
    job = job_class.perform_later('original')
    expect { process_next }.to raise_error(SidekiqRuntime::ActiveJob::RetryPersistenceError)
    expect(persisted('ready').map { |j| j['wrapped'] }).to contain_exactly(child.name, job_class.name)
    expect(persisted('schedule')).to be_empty
    expect(persisted('dead').fetch(0)['jid']).to eq(job.provider_job_id)
    expect(store.size).to eq(0)
  end

  it 'fails even when application code catches an unknown retry write result' do
    job_class = build_job do
      self.action = lambda do |*|
        retry_job(wait: 10)
      rescue RedisClient::ReadTimeoutError
        # A handled business exception cannot turn an unknown retry into success.
        nil
      end
    end
    job_class.perform_later('unknown')
    lose_client_reply
    expect { process_next }.to raise_error(SidekiqRuntime::ActiveJob::RetryPersistenceError)
    expect(persisted('schedule').size).to eq(1) # Redis wrote, reply was lost.
    expect(persisted('dead').size).to eq(1) # Duplicates are allowed; silent loss is not.
    expect(store.size).to eq(0)
  end

  it 'keeps exact Inflight when retry is refused and Dead persistence also fails' do
    job_class = retrying_job
    filter_retries
    job_class.perform_later
    config.default_capsule.redis do |redis|
      allow(redis).to receive(:multi).and_raise(RedisClient::ConnectionError, 'dead unavailable')
    end
    expect { process_next }.to raise_error(RedisClient::ConnectionError)
    expect(persisted('schedule') + persisted('dead')).to be_empty
    expect(store.entries.map(&:member)).to eq([@last_claim.member])
  end

  [:pending, :rollback].each do |transaction_state|
    it "fails an unconfirmed retry when its transaction is #{transaction_state} (framework probe)" do
      job_class = build_job
      backend = transaction_probe(job_class, open: false)
      job_class.action = lambda do |*|
        backend.open = true
        retry_job(wait: 10)
        backend.rollback if transaction_state == :rollback
      end
      job_class.perform_later
      expect { process_next }.to raise_error(SidekiqRuntime::ActiveJob::RetryPersistenceError)
      expect(persisted('schedule')).to be_empty
      expect(persisted('dead').size).to eq(1)
      expect(store.size).to eq(0)
      # For pending: even a later commit cannot retroactively justify a success ACK.
      if transaction_state == :pending
        backend.commit
        expect(persisted('schedule').size).to eq(1)
      end
    end
  end

  it 'confirms a deferred retry only if its actual write finishes inside execution (framework probe)' do
    job_class = build_job
    backend = transaction_probe(job_class, open: false)
    job_class.action = lambda do |*|
      backend.open = true
      retry_job(wait: 10)
      backend.commit
    end
    job_class.perform_later
    process_next
    expect(persisted('schedule').size).to eq(1)
    expect(persisted('dead')).to be_empty
    expect(outcomes).to eq([:retry_scheduled])
  end

  it 'rejects cross-queue retry before writing, including when the error is handled by application code' do
    job_class = build_job do
      self.action = lambda do |*|
        retry_job(queue: 'unsupported_retry_queue', wait: 10)
      rescue ArgumentError
        nil
      end
    end
    job_class.perform_later
    expect { process_next }.to raise_error(SidekiqRuntime::ActiveJob::RetryPersistenceError)
    expect(persisted('schedule')).to be_empty
    expect(persisted('dead').size).to eq(1)
    expect(config.redis { |r| r.zrange('schedule', 0, -1).any? { |raw| raw.include?('unsupported_retry_queue') } }).to be(false)
  end

  it 'fails a retry rejected after its deferred callback runs (framework probe)' do
    job_class = build_job
    backend = transaction_probe(job_class, open: false)
    filter_retries
    job_class.action = lambda do |*|
      backend.open = true
      retry_job(wait: 10)
      backend.commit
    end
    job_class.perform_later
    expect { process_next }.to raise_error(SidekiqRuntime::ActiveJob::RetryPersistenceError)
    expect(persisted('schedule')).to be_empty
    expect(persisted('dead').size).to eq(1)
  end

  it 'does not let a confirmed retry hide a second cancelled retry' do
    job_class = build_job do
      before_enqueue { throw(:abort) if arguments.first == 'cancel' }
      self.action = lambda do |*|
        retry_job(wait: 10)
        self.arguments = ['cancel']
        retry_job(wait: 10)
      end
    end
    job_class.perform_later('accepted')
    expect { process_next }.to raise_error(SidekiqRuntime::ActiveJob::RetryPersistenceError)
    expect(persisted('schedule').size).to eq(1)
    expect(persisted('dead').size).to eq(1)
  end

  it 'keeps ACK correctness independent of a broken event subscriber' do
    job_class = retrying_job
    filter_retries
    job_class.perform_later
    SidekiqRuntime.event_handler = ->(*) { raise 'broken telemetry' }
    expect { process_next }.to raise_error(SidekiqRuntime::ActiveJob::RetryPersistenceError)
    expect(persisted('dead').size).to eq(1)
  end
end
