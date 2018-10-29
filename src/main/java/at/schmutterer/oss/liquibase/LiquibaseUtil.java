package at.schmutterer.oss.liquibase;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import com.google.common.collect.Iterables;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.changelog.ChangeSet;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.CompositeResourceAccessor;
import liquibase.resource.FileSystemResourceAccessor;

public class LiquibaseUtil {

    public static final String LIQUIBASE_FILE_PATH = "OSGI-INF/liquibase/master.xml";

    protected static Liquibase createLiquibaseObject(URI changelogXml, Database liquibaseDatabase, File baseDir) throws LiquibaseException, IOException {
        File file = new File(baseDir, LIQUIBASE_FILE_PATH);
        Files.createDirectories(file.toPath().getParent());
        try (InputStream in = changelogXml.toURL().openStream()) {
            Files.copy(in, file.toPath());
        }
        return new Liquibase(
            LIQUIBASE_FILE_PATH,
            new CompositeResourceAccessor(
                new FileSystemResourceAccessor(baseDir.getAbsolutePath()),
                new ClassLoaderResourceAccessor(MigrationTestRunner.class.getClassLoader())
            ),
            liquibaseDatabase
        );
    }

    public static int findChangesetIndex(Liquibase liquibase, final String changeId) throws LiquibaseException {
        List<ChangeSet> changeSets = liquibase.listUnrunChangeSets(new Contexts(""), new LabelExpression());
        return Iterables.indexOf(changeSets, input -> Objects.equals(input.getId(), changeId));
    }

    public static Database createLiquibaseDatabase(LiquibaseConfiguration config) throws DatabaseException, SQLException {
        JdbcConnection connection = new JdbcConnection(config.getDataSource().getConnection());
        Database result = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(connection);
        result.setDefaultSchemaName(config.getSchema());
        return result;
    }

}
