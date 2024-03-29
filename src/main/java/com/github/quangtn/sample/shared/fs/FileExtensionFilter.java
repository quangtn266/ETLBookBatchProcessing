package com.github.quangtn.sample.shared.fs;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import org.apache.flink.core.fs.Path;


public class FileExtensionFilter implements Predicate<Path> {

    private final List<String> extensions;

    public FileExtensionFilter(String... extensions) { this.extensions = Arrays.asList(extensions);
    }

    @Override
    public boolean test(@Nonnull Path path) {
        String p = path.getPath();
        return extensions.stream().anyMatch(p::endsWith);
    }
}
