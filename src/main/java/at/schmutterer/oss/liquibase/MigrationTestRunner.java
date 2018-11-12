package at.schmutterer.oss.liquibase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ComputationException;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import liquibase.CatalogAndSchema;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.CompositeResourceAccessor;
import liquibase.resource.FileSystemResourceAccessor;
import lombok.extern.slf4j.Slf4j;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkField;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestClass;

@Slf4j
public class MigrationTestRunner extends BlockJUnit4ClassRunner {

    private static final Collection<String> CHANGELOG_TABLES = Lists.newArrayList(
        "DATABASECHANGELOG", "DATABASECHANGELOGLOCK", "GLOBAL_UNIQUE_ID"
    );

    private LiquibaseConfiguration config;
    private Liquibase liquibase;
    private List<String> changeIds;
    private Database database;
    private final boolean isolated;
    private final ChangelogCollection changelogCollection;

    public MigrationTestRunner(Class<?> testClass) throws InitializationError {
        super(testClass);
        List<String> changelogResourcesFromAnnotatedMethods = MigrationTestSuite.getChangelogResourcesFromAnnotatedMethods(getTestClass().getAnnotatedMethods(Changelogs.class));
        try {
            this.changelogCollection = ChangelogCollection.forResources(changelogResourcesFromAnnotatedMethods);
        } catch (LiquibaseException e) {
            throw new InitializationError(e);
        }
        this.config = Iterables.getOnlyElement(discoverDatabaseConfigurations(getTestClass()));
        isolated = true;
    }

    public MigrationTestRunner(Class<?> testClass, ChangelogCollection changelogCollection) throws InitializationError {
        super(testClass);
        this.changelogCollection = changelogCollection;
        this.config = Iterables.getOnlyElement(discoverDatabaseConfigurations(getTestClass()));
        isolated = true;
    }

    public MigrationTestRunner(Class<?> klass, ChangelogCollection changelogCollection, LiquibaseConfiguration config) throws InitializationError {
        super(klass);
        this.changelogCollection = changelogCollection;
        this.config = config;
        isolated = false;
    }

    public static void clearDatabase(LiquibaseConfiguration dataSource) throws LiquibaseException, SQLException {
        log.info("droping all database objects");
        Database liquibaseDatabase = LiquibaseUtil.createLiquibaseDatabase(dataSource);
        liquibaseDatabase.dropDatabaseObjects(
            new CatalogAndSchema(
                null,
                liquibaseDatabase.getDefaultSchemaName()
            )
        );
        liquibaseDatabase.close();
    }

    private void initLiquibase() throws InitializationError {
        try {
            database = LiquibaseUtil.createLiquibaseDatabase(config);
            List<String> requiredChangelogs = changelogCollection.getRequiredChangelogs(changeIds);
            for (String o : requiredChangelogs) {
                Liquibase liquibase1 = new Liquibase(o, new CompositeResourceAccessor(
                    new ClassLoaderResourceAccessor(),
                    new FileSystemResourceAccessor()
                ), database);
                liquibase1.update("");
            }
            Optional<String> ownChangelog = changelogCollection.findResourceForChangeSet(changeIds.stream().findFirst().get());
            liquibase = new Liquibase(ownChangelog.get(), new CompositeResourceAccessor(
                new ClassLoaderResourceAccessor(),
                new FileSystemResourceAccessor()
            ), database);
            int changesetIndex = changeIds.stream()
                .map(id -> {
                    try {
                        return LiquibaseUtil.findChangesetIndex(liquibase, id);
                    } catch (LiquibaseException e) {
                        throw new RuntimeException(e);
                    }
                })
                .min(Comparator.<Integer>naturalOrder())
                .get();
            if (changesetIndex == -1) {
                throw new RuntimeException(
                    "You've seriously messed up the order in the test suite! " +
                        "Basically this shouldn't be possible since MigrationTestSuite.class should order the tests first."
                );
            }
            liquibase.update(changesetIndex, "");
        } catch (LiquibaseException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<LiquibaseConfiguration> discoverDatabaseConfigurations(TestClass testClass) throws InitializationError {
        List<FrameworkMethod> annotatedMethods = testClass.getAnnotatedMethods(DatabaseConfiguration.class);
        List<LiquibaseConfiguration> dataSources = Lists.transform(annotatedMethods, new Function<FrameworkMethod, LiquibaseConfiguration>() {
            @Override
            public LiquibaseConfiguration apply(FrameworkMethod frameworkMethod) {
                try {
                    return (LiquibaseConfiguration) frameworkMethod.invokeExplosively(null);
                } catch (Throwable throwable) {
                    throw new ComputationException(throwable);
                }
            }
        });
        return Lists.newArrayList(Iterables.filter(dataSources, Predicates.notNull()));
    }

    @Override
    protected void collectInitializationErrors(List<Throwable> errors) {
        super.collectInitializationErrors(errors);
        MigrationTest annotation = getTestClass().getJavaClass().getAnnotation(MigrationTest.class);
        if (annotation == null) {
            errors.add(new InitializationError("can only test classes annotated with @MigrationTest"));
        }
        for (FrameworkMethod method : getTestClass().getAnnotatedMethods(DatabaseConfiguration.class)) {
            validateNoParameters(method, errors);
            validateReturnsDatasource(method, errors);
            validateStaticMethod(method, errors);
        }
    }

    private void validateNoParameters(FrameworkMethod annotatedMethod, List<Throwable> errors) {
        if (annotatedMethod.getMethod().getParameterTypes().length > 0) {
            errors.add(new InitializationError("method " + annotatedMethod + " must not have parameters"));
        }
    }

    private void validateReturnsDatasource(FrameworkMethod annotatedMethod, List<Throwable> errors) {
        if (!LiquibaseConfiguration.class.isAssignableFrom(annotatedMethod.getReturnType())) {
            errors.add(new InitializationError("method " + annotatedMethod + "must return " + LiquibaseConfiguration.class));
        }
    }

    private void validateStaticMethod(FrameworkMethod annotatedMethod, List<Throwable> errors) {
        if (!annotatedMethod.isStatic()) {
            errors.add(new InitializationError("method " + annotatedMethod + " must be static"));
        }
    }

    @Override
    protected Statement classBlock(RunNotifier notifier) {
        final Statement result = super.classBlock(notifier);
        MigrationTest annotation = getTestClass().getJavaClass().getAnnotation(MigrationTest.class);
        changeIds = Arrays.asList(annotation.value());
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                clearDatabaseIfNecessary();
                initLiquibase();
                result.evaluate();
                clearDatabaseIfNecessary();
            }
        };
    }

    public void clearDatabaseIfNecessary() throws LiquibaseException, SQLException {
        if (isolated && !Boolean.getBoolean("liquibase.test.skipDelete")) {
            clearDatabase(config);
        }
    }

    @Override
    protected Object createTest() throws Exception {
        Object test = super.createTest();
        for (FrameworkField frameworkField : getTestClass().getAnnotatedFields(LiquibaseContext.class)) {
            frameworkField.getField().setAccessible(true);
            frameworkField.getField().set(test, liquibase);
            frameworkField.getField().setAccessible(false);
        }
        for (FrameworkField frameworkField : getTestClass().getAnnotatedFields(TestDataSource.class)) {
            frameworkField.getField().setAccessible(true);
            frameworkField.getField().set(test, config.getDataSource());
            frameworkField.getField().setAccessible(false);
        }
        return test;
    }

    @Override
    protected Statement methodBlock(FrameworkMethod method) {
        return new DatabaseCleanupStatement(super.methodBlock(method));
    }

    private void cleanupDatabase(Connection connection) throws SQLException {
        cleanupChangelog(connection);
        List<String> tableNames = getTableNames(connection, config.getSchema());
        tableNames.removeAll(CHANGELOG_TABLES);
        int tries = 5;
        for (int i = 0; i < tries; i++) {
            Iterator<String> tableIterator = tableNames.iterator();
            while (tableIterator.hasNext()) {
                try (java.sql.Statement statement = connection.createStatement()) {
                    statement.execute("DELETE FROM " + tableIterator.next());
                    tableIterator.remove();
                } catch (SQLException e) {
                    // can happen because of referential integrity
                }
            }
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
            if (tableNames.isEmpty()) {
                return;
            }
        }
        log.error("Unable to clean up the database for further tests!");
    }

    public static List<String> getTableNames(Connection connection, String schema) throws SQLException {
        ResultSet tables;
        java.sql.Statement statement = null;
        if (connection.getMetaData().getDatabaseProductName().equals("Oracle")) {
            statement = connection.createStatement();
            tables = statement.executeQuery("SELECT object_name as TABLE_NAME, 'TABLE' as TABLE_TYPE FROM user_objects WHERE object_type = 'TABLE'");
        } else {
            tables = connection.getMetaData().getTables(null, schema, null, null);
        }
        try {
            List<String> result = new LinkedList<>();
            while (tables.next()) {
                if (Objects.equals(tables.getString("TABLE_TYPE"), "TABLE")) {
                    result.add(tables.getString("TABLE_NAME"));
                }
            }
            return result;
        } finally {
            if (statement != null) {
                statement.close();
            }
            tables.close();
        }
    }

    private void cleanupChangelog(Connection connection) throws SQLException {
        int firstExecuted;
        try (java.sql.Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT MIN(ORDEREXECUTED) AS FIRST_EXECUTED FROM DATABASECHANGELOG WHERE ID IN (" + changeIdsAsSqlString() + ")")
        ) {
            if (!resultSet.next()) {
                return;
            }
            firstExecuted = resultSet.getInt("FIRST_EXECUTED");
            if (firstExecuted <= 0) {
                return;
            }
        }
        try (java.sql.Statement statement = connection.createStatement()) {
            statement.executeUpdate("DELETE FROM DATABASECHANGELOG WHERE ORDEREXECUTED >= " + firstExecuted);
        }
    }

    private String changeIdsAsSqlString() {
        return "'" + Arrays.toString(changeIds.toArray())
            .replaceAll("\\[", "")
            .replaceAll("\\]", "")
            .replaceAll(" ", "")
            .replaceAll(",", "','") + "'";
    }

    private class DatabaseCleanupStatement extends Statement {
        private final Statement statement;

        public DatabaseCleanupStatement(Statement statement) {
            this.statement = statement;
        }

        @Override
        public void evaluate() throws Throwable {
            boolean shouldRethrow = true;
            try {
                statement.evaluate();
            } catch (Exception e) {
                shouldRethrow = false;
                throw e;
            } finally {
                doCleanupDatabase(shouldRethrow);
            }
        }

        public void doCleanupDatabase(boolean shouldRethrow) throws SQLException {
            try (Connection connection = config.getDataSource().getConnection()) {
                cleanupDatabase(connection);
            } catch (Exception e) {
                if (shouldRethrow) {
                    throw e;
                } else {
                    e.printStackTrace();
                }
            }
        }
    }

}
