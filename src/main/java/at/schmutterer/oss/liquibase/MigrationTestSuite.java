package at.schmutterer.oss.liquibase;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import liquibase.exception.LiquibaseException;
import liquibase.resource.AbstractResourceAccessor;
import liquibase.util.StringUtils;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.ParentRunner;
import org.junit.runners.Suite;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestClass;

public class MigrationTestSuite extends ParentRunner<Runner> {

    private List<Runner> runners = new ArrayList<>();
    private final List<LiquibaseConfiguration> dataSources;

    public MigrationTestSuite(Class<?> testClass) throws InitializationError {
        super(testClass);
        final Suite.SuiteClasses annotation = testClass.getAnnotation(Suite.SuiteClasses.class);
        final List<Class<?>> testClasses = Lists.newArrayList(annotation .value());
        ChangelogCollection changelogCollection;
        try {
            List<FrameworkMethod> annotatedMethods = new TestClass(testClass).getAnnotatedMethods(Changelogs.class);
            List<String> resources =  getChangelogResourcesFromAnnotatedMethods(annotatedMethods);
            changelogCollection = ChangelogCollection.forResources(resources);
            Collections.sort(testClasses, (o1, o2) -> {
                Integer index1 = Arrays.asList(o1.getAnnotation(MigrationTest.class).value()).stream()
                    .map(changelogCollection::indexOf)
                    .min(Comparator.naturalOrder())
                    .get();
                Integer index2 = Arrays.asList(o2.getAnnotation(MigrationTest.class).value()).stream()
                    .map(changelogCollection::indexOf)
                    .min(Comparator.naturalOrder())
                    .get();
                return index1.compareTo(index2);
            });
        } catch (LiquibaseException e) {
            throw new InitializationError(e);
        }
        dataSources = MigrationTestRunner.discoverDatabaseConfigurations(getTestClass());
        if (dataSources.isEmpty()) {
            for (Class<?> aClass : testClasses) {
                runners.add(new MigrationTestRunner(aClass, changelogCollection));
            }
        } else {
            for (LiquibaseConfiguration dataSource : dataSources) {
                for (Class<?> aClass : testClasses) {
                    runners.add(new MigrationTestRunner(aClass, changelogCollection, dataSource));
                }
            }
        }
    }

    protected static List<String> getChangelogResourcesFromAnnotatedMethods(List<FrameworkMethod> annotatedMethods) {
        List<String> result = new LinkedList<>();
        for (FrameworkMethod annotatedMethod : annotatedMethods) {
            Object invoke;
            try {
                invoke = annotatedMethod.getMethod().invoke(null);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException(e);
            }
            if (invoke instanceof Collection) {
                result.addAll((Collection<? extends String>) invoke);
            } else if (invoke instanceof String[]) {
                Collections.addAll(result, (String[]) invoke);
            }
        }
        return result;
    }

    @Override
    protected List<Runner> getChildren() {
        return runners;
    }

    @Override
    protected Description describeChild(Runner child) {
        return child.getDescription();
    }

    @Override
    protected void runChild(Runner runner, final RunNotifier notifier) {
        runner.run(notifier);
    }

    @Override
    protected Statement classBlock(RunNotifier notifier) {
        final Statement statement = super.classBlock(notifier);
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                clearAllDatabases();
                statement.evaluate();
                clearAllDatabases();
            }
        };
    }

    private void clearAllDatabases() throws LiquibaseException, SQLException {
        if (Boolean.getBoolean("liquibase.test.skipDelete")) {
            return;
        }
        for (LiquibaseConfiguration dataSource : dataSources) {
            MigrationTestRunner.clearDatabase(dataSource);
        }
    }

    private class ClassLoaderResourceAccessor extends AbstractResourceAccessor {

        private ClassLoader classLoader;

        public ClassLoaderResourceAccessor() {
            this.classLoader = getClass().getClassLoader();
            init(); //init needs to be called after classloader is set
        }

        public ClassLoaderResourceAccessor(ClassLoader classLoader) {
            this.classLoader = classLoader;
            init(); //init needs to be called after classloader is set
        }

        @Override
        public Set<InputStream> getResourcesAsStream(String path) throws IOException {
            Enumeration<URL> resources = classLoader.getResources(path);
            if (resources == null || !resources.hasMoreElements()) {
                return null;
            }
            Set<String> seenUrls = new HashSet<>();
            Set<InputStream> returnSet = new HashSet<>();
            while (resources.hasMoreElements()) {
                URL url = resources.nextElement();
                if (seenUrls.contains(url.toExternalForm())) {
                    continue;
                }
                seenUrls.add(url.toExternalForm());
                InputStream resourceAsStream = url.openStream();
                if (resourceAsStream != null) {
                    returnSet.add(resourceAsStream);
                    // for the tests we just need the first result in fact
                    return returnSet;
                }
            }

            return returnSet;
        }

        @Override
        public Set<String> list(String relativeTo, String path, boolean includeFiles, boolean includeDirectories, boolean recursive) throws IOException {
            path = convertToPath(relativeTo, path);

            URL fileUrl = classLoader.getResource(path);
            if (fileUrl == null) {
                return null;
            }

            if (!fileUrl.toExternalForm().startsWith("file:")) {
                if (fileUrl.toExternalForm().startsWith("jar:file:")
                    || fileUrl.toExternalForm().startsWith("wsjar:file:")
                    || fileUrl.toExternalForm().startsWith("zip:")) {

                    String file = fileUrl.getFile();
                    String splitPath = file.split("!")[0];
                    if (splitPath.matches("file:\\/[A-Za-z]:\\/.*")) {
                        splitPath = splitPath.replaceFirst("file:\\/", "");
                    } else {
                        splitPath = splitPath.replaceFirst("file:", "");
                    }
                    splitPath = URLDecoder.decode(splitPath, "UTF-8");
                    File zipfile = new File(splitPath);


                    File zipFileDir = FileUtil.unzip(zipfile);
                    if (path.startsWith("classpath:")) {
                        path = path.replaceFirst("classpath:", "");
                    }
                    if (path.startsWith("classpath*:")) {
                        path = path.replaceFirst("classpath\\*:", "");
                    }
                    URI fileUri = new File(zipFileDir, path).toURI();
                    fileUrl = fileUri.toURL();
                }
            }

            try {
                File file = new File(fileUrl.toURI());
                if (file.exists()) {
                    Set<String> returnSet = new HashSet<>();
                    getContents(file, recursive, includeFiles, includeDirectories, path, returnSet);
                    return returnSet;
                }
            } catch (URISyntaxException e) {
                //not a local file
            } catch (IllegalArgumentException e) {
                //not a local file
            }

            Enumeration<URL> resources = classLoader.getResources(path);
            if (resources == null || !resources.hasMoreElements()) {
                return null;
            }
            Set<String> returnSet = new HashSet<>();
            while (resources.hasMoreElements()) {
                String url = resources.nextElement().toExternalForm();
                url = url.replaceFirst("^\\Q" + path + "\\E", "");
                returnSet.add(url);
            }
            return returnSet;
        }

        @Override
        public ClassLoader toClassLoader() {
            return classLoader;
        }

        @Override
        public String toString() {
            String description;
            if (classLoader instanceof URLClassLoader) {
                List<String> urls = new ArrayList<String>();
                for (URL url : ((URLClassLoader) classLoader).getURLs()) {
                    urls.add(url.toExternalForm());
                }
                description = StringUtils.join(urls, ",");
            } else {
                description = classLoader.getClass().getName();
            }
            return getClass().getName() + "(" + description + ")";

        }
    }

}
