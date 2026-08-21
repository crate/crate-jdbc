package io.crate.client.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CrateVersionTest {

    /** What {@code select version()} answers with. */
    private static final String REPORTED = "CrateDB 6.4.1 (built 0a1b2c3/2026-01-01T00:00:00Z, "
                                           + "OpenJDK 64-Bit Server VM 24.0.2+12)";

    @Test
    public void versionIsReadFromWhatTheServerReportsItselfAs() throws SQLException {
        CrateVersion version = new CrateVersion(REPORTED);

        assertThat(version.toString(), is("6.4.1"));
        assertThat(version.major(), is(6));
        assertThat(version.minor(), is(4));
        assertThat(version.patch(), is(1));
    }

    @Test
    public void textWithoutAVersionIsRejected() {
        assertThrows(SQLException.class, () -> new CrateVersion("CrateDB (built from source)"));
    }

    /**
     * A minimum is asked for as a release, without its patch level: a
     * behavior arrives in 6.1.0 and is there in every 6.1.x after it. Version
     * numbers are compared as numbers, so 6.10 is later than 6.4.
     */
    @Test
    public void atLeastComparesTheReleaseAndNotThePatchLevel() throws SQLException {
        CrateVersion version = new CrateVersion("6.1.0");

        assertThat(version.atLeast(6, 1), is(true));
        assertThat(version.atLeast(6, 0), is(true));
        assertThat(version.atLeast(5, 9), is(true));
        assertThat(version.atLeast(6, 2), is(false));
        assertThat(version.atLeast(7, 0), is(false));

        assertThat(new CrateVersion("6.10.0").atLeast(6, 4), is(true));
        assertThat(new CrateVersion("10.0.0").atLeast(6, 4), is(true));
    }
}
