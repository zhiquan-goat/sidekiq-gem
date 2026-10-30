# frozen_string_literal: true

require 'spec_helper'
require 'json'
require 'open3'
require 'rbconfig'

RSpec.describe SidekiqRuntime::QueueKeys do
  it 'keeps real installation state unchanged after rejecting another namespace' do
    fixture = File.expand_path('../fixtures/storage_install_probe.rb', __dir__)
    stdout, stderr, status = Open3.capture3(
      RbConfig.ruby,
      '-Ilib',
      fixture,
      chdir: File.expand_path('../..', __dir__)
    )
    expect(status).to be_success, stderr
    result = JSON.parse(stdout)

    expect(result).to include(
      'lazy_default_did_not_pin' => true,
      'same_namespace_succeeded' => true,
      'different_namespace_error' => 'ArgumentError',
      'storage_still_first_object' => true,
      'event_handler_unchanged' => true,
      'fetcher_unchanged' => true,
      'configuration_unchanged' => true,
      'store_key_unchanged' => true
    )
  end
end
