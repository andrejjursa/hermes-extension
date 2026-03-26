<?php

declare(strict_types=1);

namespace Efabrica\HermesExtension\Metrics;

use Prometheus\CollectorRegistry;
use Prometheus\Exception\MetricsRegistrationException;

final class PrometheusMetric
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

    public function incrementMessageCount(string $messageType, bool $success = true): void
    {
        try {
            $counter = $this->registry->getOrRegisterCounter(
                $this->namespace,
                'hermes_event_processed_total',
                'Total number of processed events',
                ['event_type', 'status']
            );
        } catch (MetricsRegistrationException $exception) {
            return;
        }

        $counter->inc([$messageType, $success ? 'success' : 'error']);
    }
}