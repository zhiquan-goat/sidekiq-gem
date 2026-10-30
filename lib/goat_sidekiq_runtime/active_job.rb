# frozen_string_literal: true

require 'goat_sidekiq_runtime'
require 'active_job'
require_relative '../sidekiq_runtime/active_job/execution'
require_relative '../sidekiq_runtime/active_job/extensions'
require_relative '../sidekiq_runtime/active_job/job_wrapper'

ActiveSupport.on_load(:active_job) do
  prepend SidekiqRuntime::ActiveJob::Extensions
end

require_relative '../active_job/queue_adapters/sidekiq_runtime_adapter'
