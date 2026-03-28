<?php

declare(strict_types=1);

namespace Efabrica\HermesExtension\Metrics;

use Prometheus\Storage\Adapter;

interface ExpiringDataAdapter extends Adapter
{
    public function withTTL(int $ttl): ExpiringDataAdapter;

    public function withoutTTL(): ExpiringDataAdapter;
}