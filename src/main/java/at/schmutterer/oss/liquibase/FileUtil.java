package at.schmutterer.oss.liquibase;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static org.apache.commons.io.FileUtils.cleanDirectory;

/**
 * copy of old FileUtil implementation by liquibase, since these methods have been removed from there.
 */
public class FileUtil {

    public static File unzip(File zipFile) throws IOException {
        File tempDir = File.createTempFile("liquibase-unzip", ".dir");
        tempDir.delete();
        tempDir.mkdir();

        JarFile jarFile = new JarFile(zipFile);
        try {
            Enumeration<JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                File entryFile = new File(tempDir, entry.getName());
                if (!entry.isDirectory()) {
                    entryFile.getParentFile().mkdirs();
                    FileOutputStream out = new FileOutputStream(entryFile);

                    byte[] buf = new byte[1024];
                    int len;
                    InputStream inputStream = jarFile.getInputStream(entry);
                    while ((len = inputStream.read(buf)) > 0) {
                        if (!zipFile.exists()) {
                            zipFile.getParentFile().mkdirs();
                        }
                        out.write(buf, 0, len);
                    }
                    inputStream.close();
                    out.close();
                }
            }

            FileUtil.forceDeleteOnExit(tempDir);
        } finally {
            jarFile.close();
        }

        return tempDir;
    }

    private static void forceDeleteOnExit(final File file) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    FileUtil.deleteDirectory(file);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private static void deleteDirectory(final File directory) throws IOException {
        if (!directory.exists()) {
            return;
        }

        cleanDirectory(directory);
        if (!directory.delete()) {
            throw new IOException("Cannot delete " + directory.getAbsolutePath());
        }
    }


}
