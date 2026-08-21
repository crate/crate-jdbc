package io.crate.client.jdbc;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;

public class CrateDriverVersionTest {

    /**
     * The version the driver answers with is the one the artifact was built
     * as, whole and split into the parts {@code java.sql.Driver} asks for, so
     * that a release cannot ship a driver misreporting itself.
     */
    @Test
    public void reportedVersionMatchesTheVersionBeingBuilt() {
        CrateDriverVersion version = CrateDriverVersion.CURRENT;

        assertThat(version.toString(), is(System.getProperty("project.version")));
        assertThat(version.toString(), startsWith(version.major + "." + version.minor + "."));
    }
}
