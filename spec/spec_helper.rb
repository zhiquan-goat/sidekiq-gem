# frozen_string_literal: true

require 'uri'
require 'rspec'
require 'active_support'
require 'active_support/notifications'
require 'active_support/core_ext/numeric/time'
require 'active_support/core_ext/object/inclusion'
require 'goat_sidekiq_runtime'

redis_url = URI(ENV.fetch('GOAT_RUNTIME_TEST_REDIS_URL', 'redis://127.0.0.1:17779/15'))
unless %w[127.0.0.1 localhost ::1 [::1]].include?(redis_url.host) && redis_url.port != 6379
  abort 'gem specs require a dedicated loopback Redis endpoint outside default port 6379'
end
ENV['SIDEKIQ_REDIS_URL'] = redis_url.to_s

SidekiqRuntime::QueueKeys.configure!(
  SidekiqRuntime::Storage.new(namespace: 'srt:test:v1')
)
SidekiqRuntime.event_handler = ->(event, payload) { ActiveSupport::Notifications.instrument(event, payload) }

require_relative 'sidekiq_runtime_spec_support'

RSpec.configure do |config|
  config.expect_with(:rspec) { |expectations| expectations.syntax = :expect }
end
