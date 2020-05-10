package routing.demo;

import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public abstract class AbstractDynamicRoutingDataSource<V> extends AbstractDataSource {
    @NonNull
    private DataSourceLookup<V> dataSourceLookup;
    @Nullable
    private V defaultKey;
    private boolean lenientFallback;
    private DataSource defaultDataSource;


    public AbstractDynamicRoutingDataSource(DataSourceLookup<V> dataSourceLookup, V defaultKey) {
        this.dataSourceLookup = dataSourceLookup;
        this.defaultKey = defaultKey;
        if (defaultKey != null) {
            lenientFallback = true;
            defaultDataSource = dataSourceLookup.getDataSource(defaultKey);
            if (defaultDataSource == null) {
                throw new RuntimeException("defaultDataSource doesn't contain in dataSourceLookup");
            }
        }
    }


    @NonNull
    protected DataSourceLookup<V> getDataSourceLookup() {
        return dataSourceLookup;
    }

    @Nullable
    protected V getDefaultKey() {
        return defaultKey;
    }

    protected boolean isLenientFallback() {
        return lenientFallback;
    }

    protected DataSource getDefaultDataSource() {
        return defaultDataSource;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return determineTargetDataSource().getConnection();
    }

    protected DataSource determineTargetDataSource() {
        DataSource dataSource = dataSourceLookup.getDataSource(getCurrentKey());
        if (dataSource == null && lenientFallback) {
            return defaultDataSource;
        }
        return dataSource;
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return determineTargetDataSource().getConnection(username, password);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        }
        return determineTargetDataSource().unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return (iface.isInstance(this) || determineTargetDataSource().isWrapperFor(iface));
    }

    protected abstract V getCurrentKey();

}
