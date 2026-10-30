#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
version="$(ruby -e 'spec = Gem::Specification.load("goat_sidekiq_runtime.gemspec"); abort "invalid gemspec" unless spec; print spec.version')"
gem_package="goat_sidekiq_runtime-${version}.gem"

gem build goat_sidekiq_runtime.gemspec --output "$gem_package"
gem push --key github --host https://rubygems.pkg.github.com/goatapp "$gem_package"
