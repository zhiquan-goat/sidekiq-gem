# frozen_string_literal: true

require_relative 'queue_configuration'

module SidekiqRuntime
  # Deployment owns -q/-c/-t; the application supplies the reliable execution hooks.
  module ServerConfigurator
    def self.apply(config, redis_config:, recovery: nil)
      raise ArgumentError, 'redis_config is required for a Sidekiq server' unless redis_config
      installed_settings = config.instance_variable_get(:@goat_sidekiq_runtime_server_installed)
      if installed_settings
        if config[:runtime_recovery] != installed_settings || (recovery && recovery != installed_settings)
          raise ArgumentError, 'runtime recovery settings cannot change after installation'
        end
        return config
      end
      recovery ||= RecoveryConfiguration.for(config)
      raise ArgumentError, 'recovery must be RecoveryConfiguration' unless recovery.is_a?(RecoveryConfiguration)
      config[:runtime_recovery] = recovery
      Sidekiq::Processor.prepend(ProcessorBoundary) unless Sidekiq::Processor.ancestors.include?(ProcessorBoundary)

      config.redis = redis_config
      config[:fetch_class] = ReliableFetch
      config[:scheduled_enq] = AtomicScheduledEnqueuer
      reclaimer = OrphanReclaimer.new(config, settings: recovery)
      config.on(:startup) do
        validate!(config)
        reclaimer.start
        true
      end
      config.on(:quiet) { reclaimer.stop }
      config.on(:shutdown) do
        reclaimer.stop
      end
      config.instance_variable_set(:@goat_sidekiq_runtime_server_installed, recovery)
      config
    end

    def self.validate!(config)
      unless config.capsules.keys == ['default']
        raise ArgumentError, 'Sidekiq runtime requires one default capsule'
      end
      QueueConfiguration.validate!(config)
      queues = QueueKeys.configured_queues(config)
      unless config[:fetch_class] == ReliableFetch && config[:scheduled_enq] == AtomicScheduledEnqueuer
        raise ArgumentError, 'Sidekiq runtime reliable fetch and scheduled promotion must remain enabled'
      end
      validate_capsule!(config.default_capsule, queues: queues)
      installed_settings = config.instance_variable_get(:@goat_sidekiq_runtime_server_installed)
      if installed_settings && config[:runtime_recovery] != installed_settings
        raise ArgumentError, 'runtime recovery settings changed after installation'
      end
      unless Sidekiq::Client.ancestors.include?(ClientPushBoundary)
        raise ArgumentError, 'Sidekiq runtime native client push boundary must remain enabled'
      end
      true
    end

    def self.validate_capsule!(capsule, queues:)
      fetcher = capsule.fetcher
      unless fetcher.is_a?(ReliableFetch) && fetcher.configuration_matches?(capsule) &&
          fetcher.stores_by_queue.keys == queues
        raise ArgumentError, 'Sidekiq runtime default capsule must use reliable fetch for its configured queues'
      end
    end
    private_class_method :validate_capsule!
  end
end
