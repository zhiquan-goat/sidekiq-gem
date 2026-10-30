require 'spec_helper'

RSpec.describe SidekiqRuntime::RecoveryState do
  let(:entry) do
    SidekiqRuntime::InflightStore::Claim.new(source_queue: 'queue:test', job: '{}', member: "claim\n{}", owner_identity: 'owner')
  end
  let(:snapshot) { {known: true, present: true, redis_now: 10.0, owner_status: 'owner_alive', owner_identity: 'owner'} }

  it 'reports owner heartbeat, without claiming that the job is executing' do
    expect(described_class.classify(entry, snapshot: snapshot)).to include(status: 'owner_alive', recoverable: false)
    expect(described_class.classify(entry, snapshot: snapshot.merge(owner_status: 'owner_dead')))
      .to include(status: 'owner_dead', recoverable: true)
  end

  it 'does not authorize unknown, missing or changed ownership' do
    expect(described_class.classify(entry, snapshot: {known: false})).to include(status: 'unknown', recoverable: false)
    expect(described_class.classify(entry, snapshot: snapshot.merge(present: false))).to include(status: 'missing', recoverable: false)
    expect(described_class.classify(entry, snapshot: snapshot.merge(owner_identity: 'other'))).to include(status: 'owner_changed', recoverable: false)
  end
end
