/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Checks what the driver's two artifacts contain and how each behaves on the
 * classpaths it is dropped into.
 *
 * <p>Three things vary, and all three decide which classes load and in which
 * order: the artifact — {@code crate-jdbc}, which declares pgJDBC as a
 * dependency, or {@code crate-jdbc-standalone}, which bundles a relocated copy
 * of it; the arrangement — the system classpath, or a class loader of its own
 * as a tool that keeps each driver in a plugin directory would build; and
 * which class an application touches first, since registering with the
 * {@link DriverManager} happens in a class initializer. No value oracle sees
 * any of this: a fault here is a driver that answers the wrong URLs, or none,
 * while every value it does return is right.</p>
 *
 * <p>Each scenario runs in a JVM of its own, because a JVM decides class
 * initialization once. Run through the {@code verifyArtifacts} Gradle task:
 *
 * <pre>
 *   java -cp &lt;classpath&gt; devtools/VerifyArtifacts.java \
 *       &lt;artifact&gt; &lt;scenario&gt; &lt;shade prefix&gt; &lt;classpath&gt;
 * </pre>
 */
public class VerifyArtifacts {

    private static String shadedPrefix;
    private static String shadedPath;

    /**
     * Class-file namespaces that must not appear unrelocated: a second copy
     * of them on an application's classpath is what the standalone jar
     * exists to avoid.
     */
    private static final String[] MUST_BE_RELOCATED = {
        "org/postgresql/", "com/fasterxml/", "org/checkerframework/",
    };

    /**
     * Metadata describing a bundled dependency rather than this jar, which
     * misdescribes it once its classes are relocated.
     */
    private static final String[] MUST_NOT_BE_PRESENT = {
        "META-INF/maven/", "OSGI-INF/",
    };

    public static void main(String[] args) throws Exception {
        String artifact = args[0];
        String scenario = args[1];
        shadedPrefix = args[2] + ".";
        shadedPath = shadedPrefix.replace('.', '/');
        String[] classpath = args[3].split(File.pathSeparator);
        boolean standalone = artifact.equals("standalone");

        if (standalone) {
            // What the jar holds is a property of the jar, not of the
            // classpath, so it is checked once per scenario rather than once
            // per run only because each scenario is its own JVM.
            verifyContents(new File(classpath[0]));
        }
        if (scenario.equals("alone")) {
            verifyAlone(standalone);
        } else if (scenario.equals("data-source-first")) {
            verifyDataSourceLoadedFirst(standalone);
        } else if (scenario.equals("driver-class-first")) {
            verifyDriverClassLoadedFirst(standalone);
        } else if (scenario.equals("plugin-classloader")) {
            verifyInPluginClassLoader(standalone, classpath);
        } else if (scenario.equals("alongside-pg")) {
            verifyAlongsidePostgresDriver();
        } else {
            throw new IllegalArgumentException("Unknown scenario: " + scenario);
        }
        System.out.println(artifact + " artifact verified (" + scenario + ")");
    }

    private static void verifyContents(File jarFile) throws Exception {
        List<String> entries = new ArrayList<>();
        Manifest manifest;
        try (JarFile jar = new JarFile(jarFile)) {
            manifest = jar.getManifest();
            for (Enumeration<JarEntry> e = jar.entries(); e.hasMoreElements(); ) {
                entries.add(e.nextElement().getName());
            }
        }

        for (String entry : entries) {
            for (String prefix : MUST_BE_RELOCATED) {
                expect(!entry.startsWith(prefix), "unrelocated dependency class: " + entry);
            }
            for (String unwanted : MUST_NOT_BE_PRESENT) {
                expect(!entry.contains(unwanted), "dependency metadata left in the jar: " + entry);
            }
            expect(!entry.endsWith("module-info.class"),
                "module descriptor of a relocated dependency: " + entry);
            expect(!entry.startsWith("META-INF/services/")
                    || entry.equals("META-INF/services/")
                    || entry.equals("META-INF/services/java.sql.Driver"),
                "service entry naming classes that relocation has moved: " + entry);
            if (entry.startsWith("META-INF/versions/") && entry.endsWith(".class")) {
                expect(entry.contains(shadedPath), "unrelocated multi-release class: " + entry);
            }
        }

        expectPresent(entries, "META-INF/services/java.sql.Driver");
        expectPresent(entries, "io/crate/client/jdbc/CrateDriver.class");
        expectPresent(entries, shadedPath + "org/postgresql/Driver.class");
        // The driver reads its own version from here, and does so the first
        // time an application asks for it rather than at startup — so a jar
        // that shipped without it would fail in the field, not here.
        expectPresent(entries, "io/crate/client/jdbc/version.properties");
        expectPresent(entries, "META-INF/LICENSE");
        expectPresent(entries, "META-INF/NOTICE");
        expect(entries.stream().anyMatch(e -> e.startsWith("META-INF/licenses/postgresql-")),
            "the bundled pgJDBC ships without its license");

        expect(manifest.getMainAttributes().getValue("Automatic-Module-Name") != null,
            "the jar declares no Automatic-Module-Name");
        expect(manifest.getMainAttributes().getValue("Bundled-PgJdbc-Version") != null,
            "the jar does not declare which pgJDBC it bundles");
    }

    /**
     * The artifact as it sits in a tool's driver directory. Both answer crate
     * URLs. What each does with a postgresql URL is the difference between
     * them: the standalone jar's pgJDBC is a relocated copy that belongs to
     * the driver, so it stays out of the DriverManager, while the thin
     * artifact's is the published one an application put on its own classpath
     * and has to keep answering.
     */
    private static void verifyAlone(boolean standalone) throws Exception {
        Driver crateDriver = DriverManager.getDriver("jdbc:crate://localhost:5432/doc");
        expect("io.crate.client.jdbc.CrateDriver".equals(crateDriver.getClass().getName()),
            "crate URLs answered by " + crateDriver.getClass().getName());

        expectPostgresUrlsAnswered(standalone);
        if (!standalone) {
            return;
        }

        // A data source reaches the bundled pgJDBC directly, so it gets as
        // far as the connection attempt rather than failing to find a driver.
        Object dataSource = Class.forName("io.crate.client.jdbc.CrateDataSource")
            .getDeclaredConstructor().newInstance();
        dataSource.getClass().getMethod("setUrl", String.class)
            .invoke(dataSource, "crate://localhost:1/doc");
        try {
            ((javax.sql.DataSource) dataSource).getConnection();
            throw new AssertionError("a connection to a closed port succeeded");
        } catch (Exception e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            expect(!String.valueOf(cause.getMessage()).contains("No suitable driver"),
                "the data source looks its driver up in the DriverManager: " + cause.getMessage());
        }
    }

    /**
     * An application wired with a data source and nothing else. In the
     * standalone artifact building one instantiates the bundled pgJDBC driver,
     * which registers itself, and only {@link io.crate.client.jdbc.CrateDriver}'s
     * class initializer takes it back out. Nothing here loads that class on
     * purpose: the jar's single service entry names it, so the DriverManager
     * loads it before it can answer any lookup. This scenario holds that order
     * — a second service entry, or a driver lookup that skips the service
     * scan, would let the bundled pgJDBC answer {@code jdbc:postgresql://}.
     */
    private static void verifyDataSourceLoadedFirst(boolean standalone) throws Exception {
        Object dataSource = Class.forName("io.crate.client.jdbc.CrateDataSource")
            .getDeclaredConstructor().newInstance();
        dataSource.getClass().getMethod("setServerNames", String[].class)
            .invoke(dataSource, (Object) new String[]{"localhost"});
        try {
            ((javax.sql.DataSource) dataSource).getConnection();
        } catch (Exception expected) {
            // No server is listening; the connection attempt is here to load
            // everything a real one would.
        }

        expectPostgresUrlsAnswered(standalone);
    }

    /**
     * An application that names the driver class itself, as configuration that
     * predates the service entry has it do. This reaches the driver's class
     * initializer without the DriverManager's service scan, which is the other
     * way in — and the one that decides, in the standalone artifact, whether
     * the bundled pgJDBC is taken back out before anything can ask for it.
     */
    private static void verifyDriverClassLoadedFirst(boolean standalone) throws Exception {
        Class.forName("io.crate.client.jdbc.CrateDriver");

        Driver crateDriver = DriverManager.getDriver("jdbc:crate://localhost:5432/doc");
        expect("io.crate.client.jdbc.CrateDriver".equals(crateDriver.getClass().getName()),
            "crate URLs answered by " + crateDriver.getClass().getName());
        expectPostgresUrlsAnswered(standalone);
    }

    /**
     * Who answers a {@code jdbc:postgresql://} URL. In the thin artifact that
     * is the published pgJDBC the application put on its classpath, and it has
     * to still be registered: the driver takes a bundled pgJDBC out of the
     * DriverManager, and doing that to a pgJDBC it does not own would break
     * every application that uses both.
     */
    private static void expectPostgresUrlsAnswered(boolean standalone) throws Exception {
        if (standalone) {
            try {
                Driver postgres = DriverManager.getDriver("jdbc:postgresql://localhost:5432/doc");
                throw new AssertionError(
                    "postgresql URLs answered by " + postgres.getClass().getName());
            } catch (SQLException expected) {
                // The bundled pgJDBC stays out of the DriverManager.
            }
            return;
        }
        Driver postgres = DriverManager.getDriver("jdbc:postgresql://localhost:5432/doc");
        expect("org.postgresql.Driver".equals(postgres.getClass().getName()),
            "postgresql URLs answered by " + postgres.getClass().getName());
        // Named rather than imported: on the standalone classpath there is no
        // org.postgresql package for this program to compile against.
        expect((boolean) Class.forName("org.postgresql.Driver")
                .getMethod("isRegistered").invoke(null),
            "the pgJDBC the application installed was taken out of the DriverManager");
    }

    /**
     * The jar in a class loader of its own, which is how a tool that keeps
     * each driver in its own plugin directory loads it — Apache Hop, Pentaho,
     * DBeaver, a servlet container's web application.
     *
     * <p>The {@code META-INF/services} entry does not carry the driver here:
     * the DriverManager scans for services with the thread context class
     * loader, which does not reach into a loader it knows nothing about. So
     * nothing loads {@link io.crate.client.jdbc.CrateDriver} on the
     * DriverManager's behalf, and the bundled pgJDBC — which registers itself
     * the moment anything in the jar touches it — would stay registered.</p>
     *
     * <p>What is asked of the bundled driver is whether it holds a
     * registration, rather than whether the DriverManager hands it out: the
     * DriverManager only reports drivers the asking class loader can see, so
     * from out here it would answer no either way.</p>
     */
    private static void verifyInPluginClassLoader(boolean standalone, String[] classpath)
            throws Exception {
        URL[] urls = new URL[classpath.length];
        for (int i = 0; i < classpath.length; i++) {
            urls[i] = new File(classpath[i]).toURI().toURL();
        }
        // The platform loader as parent keeps the copy on this program's own
        // classpath out of the way, so the classes under test are the plugin
        // loader's and their initialization is what is being observed.
        try (URLClassLoader plugin = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader())) {
            Class.forName("io.crate.client.jdbc.CrateDataSource", true, plugin);

            String pgjdbc = standalone ? shadedPrefix + "org.postgresql.Driver" : "org.postgresql.Driver";
            Class<?> loaded = Class.forName(pgjdbc, true, plugin);
            expect(loaded.getClassLoader() == plugin,
                "the pgJDBC under test is not the plugin loader's");
            boolean registered = (boolean) loaded.getMethod("isRegistered").invoke(null);
            if (standalone) {
                expect(!registered,
                    "the bundled pgJDBC stays in the DriverManager when a data source is "
                    + "the first class an application touches");
            } else {
                expect(registered,
                    "the pgJDBC the application installed was taken out of the DriverManager");
            }

            // Reading the driver's own version reaches version.properties, which
            // is packaged rather than compiled in.
            Class<?> driver = Class.forName("io.crate.client.jdbc.CrateDriver", true, plugin);
            Object version = driver.getMethod("getMajorVersion")
                .invoke(driver.getDeclaredConstructor().newInstance());
            expect(((Integer) version) > 0, "the driver reports major version " + version);
        }
    }

    /**
     * The jar next to a stock pgJDBC, the situation in tools that ship both:
     * the two drivers stay in their own lane.
     */
    private static void verifyAlongsidePostgresDriver() throws Exception {
        Driver crateDriver = DriverManager.getDriver("jdbc:crate://localhost:5432/doc");
        expect("io.crate.client.jdbc.CrateDriver".equals(crateDriver.getClass().getName()),
            "crate URLs answered by " + crateDriver.getClass().getName());

        Driver postgresDriver = DriverManager.getDriver("jdbc:postgresql://localhost:5432/doc");
        expect("org.postgresql.Driver".equals(postgresDriver.getClass().getName()),
            "postgresql URLs answered by " + postgresDriver.getClass().getName());
        expect(!postgresDriver.getClass().getName().startsWith(shadedPrefix),
            "the bundled pgJDBC answers postgresql URLs");

        Class<?> bundled = Class.forName(shadedPrefix + "org.postgresql.Driver");
        expect(crateDriver.getClass().getSuperclass() == bundled,
            "CrateDriver does not build on the bundled pgJDBC");

        Class.forName(shadedPrefix + "com.fasterxml.jackson.databind.ObjectMapper");
    }

    private static void expectPresent(List<String> entries, String entry) {
        expect(entries.contains(entry), "the jar is missing " + entry);
    }

    private static void expect(boolean condition, String failure) {
        if (!condition) {
            throw new AssertionError(failure);
        }
    }
}
