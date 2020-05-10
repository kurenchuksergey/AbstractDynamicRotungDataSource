package routing.demo;

import javax.sql.DataSource;

public interface DataSourceLookup<T> {

    DataSource getDataSource(T key);
}
