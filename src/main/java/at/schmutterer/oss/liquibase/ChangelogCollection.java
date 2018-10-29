package at.schmutterer.oss.liquibase;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.exception.ChangeLogParseException;
import liquibase.exception.LiquibaseException;
import liquibase.parser.ChangeLogParser;
import liquibase.parser.ChangeLogParserFactory;
import liquibase.resource.CompositeResourceAccessor;
import liquibase.resource.FileSystemResourceAccessor;

public class ChangelogCollection {

    private final SortedMap<String, List<ChangeSet>> map;
    private final List<ChangeSet> allChangesets;

    public ChangelogCollection(SortedMap<String, List<ChangeSet>> map) {
        this.map = map;
        allChangesets = new ArrayList<>();
        map.values().forEach(allChangesets::addAll);
    }

    public Optional<String> findResourceForChangeSet(String changeId) {
        return map.entrySet().stream()
            .filter(entry -> entry.getValue().stream().anyMatch(c -> c.getId().equals(changeId)))
            .map(Map.Entry::getKey)
            .findFirst();
    }

    public Integer indexOf(String resource, String changeId) {
        return Iterables.indexOf(map.get(resource), c -> c.getId().equals(changeId));
    }

    public Integer indexOf(String changeId) {
        return Iterables.indexOf(allChangesets, c -> c.getId().equals(changeId));
    }

    public static ChangelogCollection forResources(List<String> r) throws LiquibaseException {
        ChangeLogParserFactory instance = ChangeLogParserFactory.getInstance();
        CompositeResourceAccessor resourceAccessor = new CompositeResourceAccessor(
            new liquibase.resource.ClassLoaderResourceAccessor(),
            new FileSystemResourceAccessor()
        );
        ChangeLogParser parser = instance.getParser("xml", resourceAccessor);
        SortedMap<String, List<ChangeSet>> collect = r.stream().collect(Collectors.toMap(
            Function.identity(),
            s -> {
                try {
                    DatabaseChangeLog changeLog = parser.parse(s, new ChangeLogParameters(), resourceAccessor);
                    return changeLog.getChangeSets();
                } catch (ChangeLogParseException e) {
                    throw new IllegalStateException(e);
                }
            }, throwingMerger(), TreeMap::new));
        return new ChangelogCollection(collect);
    }

    private static BinaryOperator<List<ChangeSet>> throwingMerger() {
        return (x, y) -> {
            throw new IllegalStateException("duplicate key found");
        };
    }

    public List<String> getRequiredChangelogs(List<String> changeId) {
        List<String> result = new LinkedList<>();
        for (Map.Entry<String, List<ChangeSet>> stringListEntry : map.entrySet()) {
            if (stringListEntry.getValue().stream().anyMatch(c -> changeId.contains(c.getId()))) {
                break;
            }
            result.add(stringListEntry.getKey());
        }
        return result;
    }
}
