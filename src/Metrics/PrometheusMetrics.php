<?php

declare(strict_types=1);

namespace Efabrica\HermesExtension\Metrics;

use Prometheus\CollectorRegistry;
use Prometheus\Counter;
use Prometheus\Exception\MetricsRegistrationException;
use Prometheus\Gauge;

final class PrometheusMetrics
{
    protected CollectorRegistry $registry;
    protected string $namespace;

    public function __construct(
        CollectorRegistry $registry,
        string $namespace
    ) {
        $this->registry = $registry;
        $this->namespace = $namespace;
    }

    public function incrementMessageCounter(string $messageType, bool $success = true): void
    {
        try {
            $counter = $this->getEventsCounter();
        } catch (MetricsRegistrationException $exception) {
            return;
        }

        $counter->inc([$messageType, $success ? 'success' : 'error']);
    }

    public function startProcessingMessage(string $messageType): void
    {
        try {
            $gauge = $this->getEventsInProgressGauge();
        } catch (MetricsRegistrationException $exception) {
            return;
        }

        $gauge->inc([$messageType]);
    }

    public function finishProcessingMessage(string $messageType): void
    {
        try {
            $gauge = $this->getEventsInProgressGauge();
        } catch (MetricsRegistrationException $exception) {
            return;
        }

        $gauge->dec([$messageType]);
    }

    /**
     * @throws MetricsRegistrationException
     */
    private function getEventsCounter(): Counter
    {
        return $this->registry->getOrRegisterCounter(
            $this->namespace,
            'hermes_event_processed_total',
            'Total number of processed events',
            ['event_type', 'status']
        );
    }

    /**
     * @throws MetricsRegistrationException
     */
    private function getEventsInProgressGauge(): Gauge
    {
        return $this->registry->getOrRegisterGauge(
            $this->namespace,
            'hermes_events_in_progress',
            'Currently processing events',
            ['event_type']
        );
    }
}