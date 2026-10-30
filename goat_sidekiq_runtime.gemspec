# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = 'goat_sidekiq_runtime'
  spec.version = '0.3.0'
  spec.summary = 'Reliable Sidekiq transport and optional uniqueness locks'
  spec.authors = ['GOAT']
  spec.licenses = ['Nonstandard']
  spec.homepage = 'https://github.com/goatapp/ruby-services'
  spec.required_ruby_version = '>= 3.4.7'
  spec.metadata['homepage_uri'] = spec.homepage
  spec.metadata['source_code_uri'] = "#{spec.homepage}/tree/main/goat-sidekiq-runtime"
  spec.metadata['github_repo'] = 'git@github.com:goatapp/ruby-services.git'
  spec.metadata['allowed_push_host'] = 'https://rubygems.pkg.github.com/goatapp'
  spec.files = Dir.chdir(__dir__) { Dir['lib/**/*.rb', 'README.md'] }
  spec.require_paths = ['lib']
  spec.add_dependency('sidekiq', '= 7.3.10')
  spec.add_dependency('activesupport', '~> 7.2')
end
