<?php

declare(strict_types=1);

namespace Efabrica\HermesExtension\Metrics;

use Prometheus\CollectorRegistry;
use Prometheus\Counter;
use Prometheus\Exception\MetricsRegistrationException;
use Prometheus\Gauge;
use Prometheus\Histogram;

final class PrometheusMetrics
{
    protected CollectorRegistry $registry;
    protected string $namespace;

    protected int $timeStart = 0;

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
        $this->timeStart = hrtime(true);
        try {
            $gauge = $this->getEventsInProgressGauge();
        } catch (MetricsRegistrationException $exception) {
            return;
        }

        $gauge->inc([$messageType]);
    }

    public function finishProcessingMessage(string $messageType): void
    {
        /** @var int $end */
        $end = hrtime(true);
        $seconds = ($end - $this->timeStart) / 1e9;
        try {
            $gauge = $this->getEventsInProgressGauge();
        } catch (MetricsRegistrationException $exception) {
            return;
        }

        $gauge->dec([$messageType]);

        try {
            $gauge = $this->getEventTimestampGauge();
        } catch (MetricsRegistrationException $exception) {
            return;
        }

        $gauge->set(time(), [$messageType]);

        try {
            $histogram = $this->getEventDurationHistogram();
        } catch (MetricsRegistrationException $exception) {
            return;
        }

        $histogram->observe($seconds, [$messageType]);
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

    /**
     * @throws MetricsRegistrationException
     */
    private function getEventTimestampGauge(): Gauge
    {
        return $this->registry->getOrRegisterGauge(
            $this->namespace,
            'hermes_last_event_timestamp_seconds',
            'Unix timestamp of last processed event',
            ['event_type']
        );
    }

    /**
     * @throws MetricsRegistrationException
     */
    private function getEventDurationHistogram(): Histogram
    {
        return $this->registry->getOrRegisterHistogram(
            $this->namespace,
            'hermes_event_duration_seconds',
            'Event processing duration in seconds',
            ['event_type']
        );
    }
}