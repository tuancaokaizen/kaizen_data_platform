package com.kaizen.stream.architecture;

import com.kaizen.stream.processing.job.JobRunner;
import com.kaizen.stream.processing.job.OperatorNames;
import com.kaizen.stream.processing.job.StreamJob;
import com.kaizen.stream.testing.JobGraph;
import org.apache.flink.api.dag.Transformation;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Structure rules of source_code_structure.md §9, checked on the source files so a wrong move fails the build.
 * Quy tắc cấu trúc trong source_code_structure.md §9, kiểm tra trên mã nguồn để đặt sai chỗ là build lỗi.
 */
class ArchitectureTest {

    private static final Path MAIN = Paths.get("src/main/java/com/kaizen/stream");
    private static final Pattern IMPORT = Pattern.compile("^import (?:static )?([\\w.]+)", Pattern.MULTILINE);
    private static final Pattern NAME_LITERAL = Pattern.compile("String name\\(\\) \\{\\s*return \"([a-z0-9-]+)\";");

    /** Libraries that only connector classes may touch. / Thư viện chỉ lớp connector được dùng. */
    private static final List<String> CONNECTOR_LIBRARIES = List.of(
            "org.apache.flink.connector.", "org.apache.kafka.", "java.sql.", "org.elasticsearch.", "org.apache.http.",
            "com.clickhouse.", "org.postgresql.", "com.kaizen.stream.utils.connector.");

    /** Abbreviations replaced by full words (§2 rule 7). / Viết tắt đã thay bằng từ đầy đủ. */
    private static final Set<String> BANNED_WORDS = Set.of(
            "Txn", "Doc", "Biz", "Rt", "Srv", "Dlq", "Es", "Ch", "Pg", "Props", "Cfg", "Mgr", "Util", "Utils");

    @Test
    void businessCodeDoesNotDependOnSimulation() {
        assertNoImports(file -> !relative(file).startsWith("simulation/"), "com.kaizen.stream.simulation.");
    }

    @Test
    void sharedLayersDoNotDependOnJobs() {
        for (String layer : List.of("utils/", "model/", "config/")) {
            assertNoImports(file -> relative(file).startsWith(layer), "com.kaizen.stream.processing.");
            assertNoImports(file -> relative(file).startsWith(layer), "com.kaizen.stream.simulation.");
        }
        assertNoImports(file -> relative(file).startsWith("utils/"), "com.kaizen.stream.model.realtime.");
    }

    @Test
    void jobsOnlyWireSourcesTransformsAndSinks() {
        for (Path job : jobFiles()) {
            for (String imported : imports(job)) {
                for (String library : CONNECTOR_LIBRARIES) {
                    assertTrue(!imported.startsWith(library), relative(job) + " imports " + imported
                            + "; move it into a source/ or sink/ class");
                }
            }
        }
    }

    @Test
    void sourceAndSinkFoldersHoldOnlySourcesAndSinks() {
        Pattern pipelineFolder = Pattern.compile("(processing|simulation)/[a-z]+/(source|sink)/([A-Za-z]+)\\.java");
        for (Path file : javaFiles()) {
            Matcher m = pipelineFolder.matcher(relative(file));
            if (m.matches()) {
                String suffix = m.group(2).equals("source") ? "Source" : "Sink";
                assertTrue(m.group(3).endsWith(suffix), relative(file) + " must end with " + suffix
                        + " (helpers go to a sub-folder such as sink/step/ or source/store/)");
            }
        }
    }

    @Test
    void classNamesHaveNoAbbreviations() {
        for (Path file : javaFiles()) {
            String className = file.getFileName().toString().replace(".java", "");
            for (String word : className.split("(?<=[a-z0-9])(?=[A-Z])")) {
                assertTrue(!BANNED_WORDS.contains(word), relative(file) + " uses the abbreviation " + word);
            }
        }
    }

    @Test
    void everyJobDeclaresGroupAndNameMatchingItsFolder() throws Exception {
        List<Path> jobs = jobFiles();
        assertEquals(7, jobs.size(), jobs.toString());
        Set<String> fullNames = new TreeSet<>();
        for (Path file : jobs) {
            StreamJob job = instantiate(file);
            String folderGroup = relative(file).substring(0, relative(file).indexOf('/'));
            assertEquals(folderGroup, job.group(), relative(file) + " group() must match its folder");
            // deploy_stream.sh reads name() as a literal / deploy_stream.sh đọc name() dạng chuỗi hằng
            Matcher literal = NAME_LITERAL.matcher(read(file));
            assertTrue(literal.find() && literal.group(1).equals(job.name()),
                    relative(file) + " must return its name as a string literal");
            assertTrue(fullNames.add(JobRunner.fullName(job)), "duplicate job " + JobRunner.fullName(job));
            // deploy_stream.sh submits with `flink run -c <class>` / deploy_stream.sh chạy bằng `flink run -c <class>`
            job.getClass().getMethod("main", String[].class);
        }
    }

    @Test
    void everyOperatorIsNamedAsSourceTransformOrSink() throws Exception {
        for (Path file : jobFiles()) {
            for (Transformation<?> operator : JobGraph.operators(instantiate(file))) {
                String uid = operator.getUid();
                assertTrue(uid != null && OperatorNames.PATTERN.matcher(uid).matches(),
                        relative(file) + ": operator '" + operator.getName() + "' has uid " + uid);
                assertEquals(uid, operator.getName(), relative(file) + ": name and uid must be equal");
            }
        }
    }

    // ---------------------------------------------------------------- helpers / hàm phụ

    private interface FileFilter {
        boolean test(Path file);
    }

    private static void assertNoImports(FileFilter filter, String forbiddenPrefix) {
        List<String> offenders = new ArrayList<>();
        for (Path file : javaFiles()) {
            if (filter.test(file)) {
                for (String imported : imports(file)) {
                    if (imported.startsWith(forbiddenPrefix)) {
                        offenders.add(relative(file) + " → " + imported);
                    }
                }
            }
        }
        assertTrue(offenders.isEmpty(), "Forbidden imports of " + forbiddenPrefix + ": " + offenders);
    }

    /** Job classes live in processing/&lt;pipeline&gt;/ or simulation/&lt;pipeline&gt;/. / Vị trí lớp job. */
    private static List<Path> jobFiles() {
        Pattern jobPath = Pattern.compile("(processing|simulation)/[a-z]+/[A-Za-z]+Job\\.java");
        return javaFiles().stream()
                .filter(file -> jobPath.matcher(relative(file)).matches())
                // processing/job/ is the job framework, not a job / processing/job/ là khung chạy job, không phải job
                .filter(file -> !relative(file).startsWith("processing/job/"))
                .sorted()
                .collect(Collectors.toList());
    }

    private static StreamJob instantiate(Path file) throws Exception {
        String className = "com.kaizen.stream." + relative(file).replace(".java", "").replace('/', '.');
        return (StreamJob) Class.forName(className).getDeclaredConstructor().newInstance();
    }

    private static List<Path> javaFiles() {
        try (Stream<Path> files = Files.walk(MAIN)) {
            return files.filter(file -> file.toString().endsWith(".java")).collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static List<String> imports(Path file) {
        List<String> result = new ArrayList<>();
        Matcher m = IMPORT.matcher(read(file));
        while (m.find()) {
            result.add(m.group(1));
        }
        return result;
    }

    private static String read(Path file) {
        try {
            return new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String relative(Path file) {
        return MAIN.relativize(file).toString().replace('\\', '/');
    }
}
