package io.crate.client.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CrateDriverTest {

    @Test
    public void processUrlRewritesCrateSchemes() {
        assertThat(CrateDriver.processURL("crate://localhost:5432/"), is("jdbc:postgresql://localhost:5432/"));
        assertThat(CrateDriver.processURL("jdbc:crate://localhost:5432/"), is("jdbc:postgresql://localhost:5432/"));
        assertThat(CrateDriver.processURL("crate://crate1.local:5432/"), is("jdbc:postgresql://crate1.local:5432/"));
        assertThat(CrateDriver.processURL("jdbc:crate://crate1.local:5432/"), is("jdbc:postgresql://crate1.local:5432/"));
        assertThat(CrateDriver.processURL("crate://h:1"), is("jdbc:postgresql://h:1"));
        assertThat(CrateDriver.processURL("jdbc:crate://h:1"), is("jdbc:postgresql://h:1"));
    }

    @Test
    public void processUrlIsCaseInsensitiveOnTheScheme() {
        assertThat(CrateDriver.processURL("CRATE://h:1"), is("jdbc:postgresql://h:1"));
        assertThat(CrateDriver.processURL("JDBC:CRATE://h:1"), is("jdbc:postgresql://h:1"));
        assertThat(CrateDriver.processURL("Crate://h:1"), is("jdbc:postgresql://h:1"));
    }

    @Test
    public void processUrlRewritesOnlyTheLeadingScheme() {
        assertThat(CrateDriver.processURL("crate://h:1/doc?fallback=jdbc:crate://other:2"),
            is("jdbc:postgresql://h:1/doc?fallback=jdbc:crate://other:2"));
        assertThat(CrateDriver.processURL("jdbc:crate://h:1/doc?fallback=jdbc:crate://other:2"),
            is("jdbc:postgresql://h:1/doc?fallback=jdbc:crate://other:2"));
    }

    @Test
    public void processUrlRejectsForeignSchemes() {
        assertThat(CrateDriver.processURL("postgres://localhost:5432/"), nullValue());
        assertThat(CrateDriver.processURL("jdbc:postgresql://localhost:5432/"), nullValue());
        assertThat(CrateDriver.processURL("jdbc://postgres://localhost:5432/"), nullValue());
        assertThat(CrateDriver.processURL("foo://localhost:5432/"), nullValue());
        assertThat(CrateDriver.processURL("foo://h/?u=jdbc:crate://h:1"), nullValue());
        assertThat(CrateDriver.processURL(null), nullValue());
    }

    @Test
    public void processUrlAcceptsBareScheme() {
        assertThat(CrateDriver.processURL("crate://"), is("jdbc:postgresql://"));
        assertThat(CrateDriver.processURL("jdbc:crate://"), is("jdbc:postgresql://"));
    }

    /**
     * The scheme is what decides, and it is the whole scheme: a URL that only
     * starts like one this driver answers still is not one.
     */
    @Test
    public void processUrlRejectsWhatOnlyLooksLikeAScheme() {
        assertThat(CrateDriver.processURL(""), nullValue());
        assertThat(CrateDriver.processURL("   "), nullValue());
        assertThat(CrateDriver.processURL("crate:"), nullValue());
        assertThat(CrateDriver.processURL("jdbc:crate:"), nullValue());
        assertThat(CrateDriver.processURL("crate:/localhost:5432/"), nullValue());
        assertThat(CrateDriver.processURL(" crate://localhost:5432/"), nullValue());
    }

    /**
     * The URLs the driver answers are the ones it can rewrite, and it
     * describes the properties of those alone.
     */
    @Test
    public void acceptedUrlsAreTheRewritableOnes() {
        CrateDriver driver = new CrateDriver();

        assertThat(driver.acceptsURL("jdbc:crate://localhost:5432/doc"), is(true));
        assertThat(driver.acceptsURL("jdbc:postgresql://localhost:5432/doc"), is(false));
        assertThat(driver.acceptsURL(null), is(false));

        assertThat(driver.getPropertyInfo("jdbc:crate://localhost:5432/doc", null).length,
            is(greaterThan(0)));
        assertThat(driver.getPropertyInfo("jdbc:postgresql://localhost:5432/doc", null).length, is(0));
        assertThat(driver.getPropertyInfo(null, null).length, is(0));
    }

    /**
     * A URL this driver answers but cannot be read is reported as the URL the
     * caller wrote, not as the {@code jdbc:postgresql://} form it is rewritten
     * to before pgJDBC ever sees it.
     */
    @Test
    public void anUnreadableUrlIsReportedInTheCallersScheme() {
        CrateDriver driver = new CrateDriver();

        for (String url : new String[]{
                // A host list has to be closed with a slash.
                "jdbc:crate://localhost:5432",
                "crate://localhost:5432",
                // More path than a schema.
                "jdbc:crate://localhost:5432/doc/extra"}) {
            SQLException unreadable = assertThrows(SQLException.class,
                () -> driver.connect(url, null));
            assertThat(unreadable.getMessage(), containsString(url));
            assertThat(unreadable.getMessage(), not(containsString("jdbc:postgresql://")));
            assertThat(unreadable.getSQLState(), is("08001"));
        }
    }

    @Test
    public void crateDefaultsFillInOnlyWhatTheCallerLeftOut() {
        Properties properties = new Properties();
        properties.setProperty("loadBalanceHosts", "false");

        Properties withDefaults = CrateDriver.withDefaults(properties);

        assertThat(withDefaults.getProperty("loadBalanceHosts"), is("false"));
        assertThat(withDefaults.getProperty("assumeMinServerVersion"), is("9.5"));
        assertThat(properties.getProperty("assumeMinServerVersion"), nullValue());
    }

    /**
     * A URL without a path segment leaves pgJDBC to fill in the schema, and
     * what it fills in is the user name. CrateDB's default schema is doc.
     */
    @Test
    public void crateDefaultsApplyWithoutCallerProperties() {
        Properties withDefaults = CrateDriver.withDefaults(null);

        assertThat(withDefaults.getProperty("loadBalanceHosts"), is("true"));
        assertThat(withDefaults.getProperty("assumeMinServerVersion"), is("9.5"));
        assertThat(withDefaults.getProperty("PGDBNAME"), is("doc"));
    }

    @Test
    public void driverManagerRoutesEachSchemeToItsOwnDriver() throws SQLException {
        assertThat(DriverManager.getDriver("jdbc:crate://localhost:5432/doc"),
            is(instanceOf(CrateDriver.class)));
        assertThat(DriverManager.getDriver("jdbc:postgresql://localhost:5432/doc"),
            is(not(instanceOf(CrateDriver.class))));
    }

    /**
     * The driver registers itself when its class is loaded, and can be taken
     * out of the DriverManager and put back, as an application
     * container does when it unloads the classes that brought it in. Neither
     * step can happen twice.
     */
    @Test
    public void theDriverCanBeDeregisteredAndRegisteredAgain() throws SQLException {
        assertThat(CrateDriver.isRegistered(), is(true));
        assertThrows(IllegalStateException.class, CrateDriver::register);

        CrateDriver.deregister();
        try {
            assertThat(CrateDriver.isRegistered(), is(false));
            assertThrows(IllegalStateException.class, CrateDriver::deregister);
            assertThrows(SQLException.class,
                () -> DriverManager.getDriver("jdbc:crate://localhost:5432/doc"));
        } finally {
            CrateDriver.register();
        }
        assertThat(DriverManager.getDriver("jdbc:crate://localhost:5432/doc"),
            is(instanceOf(CrateDriver.class)));
    }

}
