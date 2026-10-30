require 'spec_helper'

RSpec.describe SidekiqRuntime::Recovery do
  let(:payload) { build_sidekiq_runtime_job_payload.merge('class' => 'UnloadedRecoveryJob') }

  it 'preserves original arguments and metadata without invoking business code' do
    unique = {'identity' => [123], 'queue' => payload['queue']}
    original = payload.merge('sidekiq_runtime' => {'uniqueness' => unique})
    recovered = Sidekiq.load_json(described_class.prepare(Sidekiq.dump_json(original)))
    expect(recovered.except('sidekiq_runtime')).to eq(original.except('sidekiq_runtime'))
    expect(recovered['sidekiq_runtime']).to eq('uniqueness' => unique, 'redelivered' => true)
  end

  it 'rejects malformed envelopes and unsupported routing before moving the claim' do
    expect { described_class.prepare('not-json') }.to raise_error(JSON::ParserError)
    expect { described_class.prepare('{"class":"Job","args":{}}') }.to raise_error(ArgumentError)
    raw = Sidekiq.dump_json(payload.merge('retry_queue' => 'elsewhere'))
    expect { described_class.prepare(raw) }.to raise_error(ArgumentError, /retry_queue/)
  end
end
