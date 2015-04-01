package com.datastax.driver.core;

import java.net.InetSocketAddress;
import java.util.Collection;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ClusterDelegateTest extends CCMBridge.PerClassSingleNodeCluster {
    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList();
    }

    @Test(groups = "short")
    public void should_allow_subclass_to_delegate_to_other_instance() {
        DelegatingCluster delegatingCluster = new DelegatingCluster(cluster);

        ResultSet rs = delegatingCluster.connect().execute("select * from system.local");

        assertThat(rs.all()).hasSize(1);
    }

    @Test(groups = "short", expectedExceptions = IllegalStateException.class)
    public void should_fail_if_delegating_subclass_has_not_overriden_public_method() {
        BrokenDelegatingCluster brokenCluster = new BrokenDelegatingCluster(cluster);
        brokenCluster.connect();
    }

    /**
     * Demonstrates how to write a {@link Cluster} subclass that wraps another instance (delegate / decorator pattern).
     *
     * Cluster should really be an interface (which would have made this process trivial), but for historical reasons
     * it's a class.
     *
     * To avoid initializing the internal state in the parent class, {@code super.closeAsync()} must be called in this
     * class's constructor. Then <em>all</em> public methods must be overriden to delegate to the wrapped instance.
     */
    static class DelegatingCluster extends Cluster {

        private final Cluster delegate;

        public DelegatingCluster(Cluster delegate) {
            super(delegate.getClusterName(), Lists.<InetSocketAddress>newArrayList(), delegate.getConfiguration());
            this.delegate = delegate;
            super.closeAsync();
        }

        @Override
        public Cluster init() {
            return delegate.init();
        }

        @Override
        public Session newSession() {
            return delegate.newSession();
        }

        @Override
        public Session connect() {
            return delegate.connect();
        }

        @Override
        public Session connect(String keyspace) {
            return delegate.connect(keyspace);
        }

        @Override
        public Metadata getMetadata() {
            return delegate.getMetadata();
        }

        @Override
        public Configuration getConfiguration() {
            return delegate.getConfiguration();
        }

        @Override
        public Metrics getMetrics() {
            return delegate.getMetrics();
        }

        @Override
        public Cluster register(Host.StateListener listener) {
            return delegate.register(listener);
        }

        @Override
        public Cluster unregister(Host.StateListener listener) {
            return delegate.unregister(listener);
        }

        @Override
        public Cluster register(LatencyTracker tracker) {
            return delegate.register(tracker);
        }

        @Override
        public Cluster unregister(LatencyTracker tracker) {
            return delegate.unregister(tracker);
        }

        @Override
        public CloseFuture closeAsync() {
            return delegate.closeAsync();
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        public boolean isClosed() {
            return delegate.isClosed();
        }
    }

    /**
     * A broken implementation that does not properly override all public methods. It will fail when used.
     */
    static class BrokenDelegatingCluster extends Cluster {

        private final Cluster delegate;

        public BrokenDelegatingCluster(Cluster delegate) {
            super(delegate.getClusterName(), Lists.<InetSocketAddress>newArrayList(), delegate.getConfiguration());
            this.delegate = delegate;
            super.closeAsync();
        }
    }
}
