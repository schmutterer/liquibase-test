# liquibase-test
Project helping with the test of liquibase changesets

# Build the project

To build the project it's required having access to the oracle maven repository. The following link describes how this could be achived.

[1] https://blogs.oracle.com/dev2dev/get-oracle-jdbc-drivers-and-ucp-from-oracle-maven-repository-without-ides

# Usage

The project does not have to be build manually. It could be directly used by following the next steps:

## Add maven dependency to your test project

``` xml
<dependency>
  <groupId>at.schmutterer.oss.liquibase</groupId>
  <artifactId>liquibase-test</artifactId>
  <version>CURRENT_VERSION</version>
  <scope>test</scope>
  <exclusions>
    <exclusion>
      <groupId>com.oracle.jdbc</groupId>
      <artifactId>ojdbc8</artifactId>
    </exclusion>
  </exclusions>
</dependency>
```

If you need oracle tests you either need to register your repository as described here [1] or just exclude it completely and use a version from your own repository.

[1] https://blogs.oracle.com/dev2dev/get-oracle-jdbc-drivers-and-ucp-from-oracle-maven-repository-without-ides

## Add an abstract test class

This one is optional but should give you a good idea about what is required to use this lib:

``` java
package at.schmutterer.oss.liquibasetestexample;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import javax.sql.DataSource;

import at.schmutterer.oss.liquibase.Changelogs;
import at.schmutterer.oss.liquibase.DatabaseConfiguration;
import at.schmutterer.oss.liquibase.LiquibaseConfiguration;
import at.schmutterer.oss.liquibase.LiquibaseContext;
import at.schmutterer.oss.liquibase.MigrationTestRunner;
import at.schmutterer.oss.liquibase.TestDataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.changelog.ChangeSet;
import liquibase.exception.LiquibaseException;
import lombok.extern.slf4j.Slf4j;
import oracle.jdbc.pool.OracleDataSource;
import org.dalesbred.Database;
import org.h2.jdbcx.JdbcDataSource;
import org.joda.time.LocalDate;
import org.joda.time.base.AbstractInstant;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;

@Slf4j
@RunWith(MigrationTestRunner.class)
public abstract class AbstractMigrationTest {

    @LiquibaseContext
    protected Liquibase liquibase;
    @TestDataSource
    protected DataSource dataSource;

    protected Connection connection;

    protected Database database;

    @Changelogs
    public static String[] changelogs() throws IOException, URISyntaxException {
        return new String[]{"PATH/TO/LIQUIBASE/FILE1.xml", "PATH/TO/LIQUIBASE/FILE2.xml"};
    }

    @Before
    public void initDalesbred() throws Exception {
        // dalesbread is optional but should help you with your tests
        connection = dataSource.getConnection();
        database = Database.forDataSource(dataSource);
    }

    @After
    public void disconnectDB() throws Exception {
        // disconnect dalesbread - delete this if you dont use dalesbred
        connection.close();
    }

    @DatabaseConfiguration
    public static LiquibaseConfiguration dataSource() throws SQLException {
        // logic to create a datasource goes here
        JdbcDataSource jdbcDataSource = new JdbcDataSource();
        jdbcDataSource.setURL("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
        return new LiquibaseConfiguration(jdbcDataSource, "PUBLIC");
    }

    protected void runNextChange() throws LiquibaseException {
        // helper method - could also be included in your tests
        runNextChanges(1);
    }

    protected void runNextChanges(int num) throws LiquibaseException {
        // helper method - could also be included in your tests
        List<ChangeSet> changeSets = liquibase.listUnrunChangeSets(new Contexts(""), new LabelExpression());
        int alwaysRunInTheWay = 0;
        for (ChangeSet changeSet : changeSets) {
            if (changeSet.isAlwaysRun()) {
                alwaysRunInTheWay++;
            } else {
                break;
            }
        }
        liquibase.update(num + alwaysRunInTheWay, null);
    }

}
```

## Write your own test

If you're using the abstract class the test could look like this here:

``` java
package at.schmutterer.oss.liquibasetestexample;

import at.schmutterer.oss.liquibase.MigrationTest;
import org.junit.Test;

// liquibase-test runs every change to this number, but not the number itself!
@MigrationTest("CHANGE_ID_TO_TEST")
public class SimpleTest extends AbstractMigrationTest {

    @Test
    public void testCopiesEntries() throws Exception {
        // setup test data here

        // this executes the next change - which will be CHANGE_ID_TO_TEST
        runNextChange();

        // use dalesbred for example here to validate the changes applied by the change
    }

}
```

## Run your test suite

To run all tests as a suite use

```
package at.schmutterer.oss.liquibasetestexample;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import at.schmutterer.oss.liquibase.Changelogs;
import at.schmutterer.oss.liquibase.DatabaseConfiguration;
import at.schmutterer.oss.liquibase.LiquibaseConfiguration;
import at.schmutterer.oss.liquibase.MigrationTestSuite;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(MigrationTestSuite.class)
@Suite.SuiteClasses({
    SimpleTest.class
})
public class AllMigrationsTest {

    @DatabaseConfiguration
    public static LiquibaseConfiguration dataSource() throws SQLException {
        return AbstractMigrationTest.dataSource();
    }

    @Changelogs
    public static List<String> changelogs() throws IOException, URISyntaxException {
        return Arrays.asList("PATH/TO/LIQUIBASE/FILE1.xml", "PATH/TO/LIQUIBASE/FILE2.xml");
    }

}
```
