# frozen_string_literal: true

module Workers
  module Daemons
    class Upstream < Base
      def run
        Engine.all.map { |e| Thread.new { process(e) } }.map(&:join)
      end

      def process(engine)
        EM.synchrony do
          upstream = Peatio::Upstream.registry[engine.driver]
          engine.markets.each do |market|
            configs = engine.data.merge('source' => market.id, 'amqp' => ::AMQP::Queue, 'target' => market.data['target'])
            upstream.new(configs).ws_connect
            Rails.logger.info "Upstream #{market.data['upstream']} started"
          rescue StandardError => e
            report_exception(e)
            next
          end
        end
      end

      def stop
        puts 'Shutting down'
        @shutdown = true
        exit(42)
      end
    end
  end
end
