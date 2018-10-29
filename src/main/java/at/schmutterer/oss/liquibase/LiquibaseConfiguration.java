package at.schmutterer.oss.liquibase;

import javax.sql.DataSource;

import lombok.AllArgsConstructor;
import lombok.Value;

@AllArgsConstructor
@Value
public class LiquibaseConfiguration {

    private final DataSource dataSource;
    private final String schema;

}
