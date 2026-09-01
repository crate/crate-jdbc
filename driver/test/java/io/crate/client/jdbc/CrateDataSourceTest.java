package io.crate.client.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.naming.Reference;
import java.util.Hashtable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CrateDataSourceTest {

    /** A data source is configured from a URL in either driver's scheme. */
    @ParameterizedTest
    @ValueSource(strings = {
        "crate://crate1.local:5432/doc?user=crate",
        "jdbc:crate://crate1.local:5432/doc?user=crate",
        "jdbc:postgresql://crate1.local:5432/doc?user=crate"})
    public void aUrlInEitherSchemeConfiguresTheDataSource(String url) {
        CrateDataSource dataSource = new CrateDataSource();
        dataSource.setUrl(url);

        assertThat(dataSource.getServerNames()[0], is("crate1.local"));
        assertThat(dataSource.getPortNumbers()[0], is(5432));
        assertThat(dataSource.getDatabaseName(), is("doc"));
        assertThat(dataSource.getUser(), is("crate"));
    }

    /**
     * A URL in no scheme this driver or pgJDBC answers is refused where it is
     * set, rather than at the first attempt to open a connection with it.
     */
    @Test
    public void aUrlNeitherDriverCanReadIsRefused() {
        CrateDataSource dataSource = new CrateDataSource();

        assertThrows(IllegalArgumentException.class, () -> dataSource.setUrl("mysql://h:3306/db"));
        assertThrows(IllegalArgumentException.class, () -> dataSource.setURL("crate://no-slash"));
    }

    /**
     * A data source bound into a directory is stored as its properties and
     * the name of a factory, and comes back through that factory as a data
     * source that still carries the CrateDB behavior.
     */
    @Test
    public void aDataSourceSurvivesBeingBoundIntoADirectory() throws Exception {
        CrateDataSource bound = new CrateDataSource();
        bound.setUrl("crate://crate1.local:5432/doc");
        bound.setUser("crate");

        Reference reference = bound.getReference();
        Object looked = new CrateDataSourceFactory()
            .getObjectInstance(reference, null, null, new Hashtable<>());

        assertThat(looked, is(instanceOf(CrateDataSource.class)));
        CrateDataSource resolved = (CrateDataSource) looked;
        assertThat(resolved.getServerNames()[0], is("crate1.local"));
        assertThat(resolved.getPortNumbers()[0], is(5432));
        assertThat(resolved.getDatabaseName(), is("doc"));
        assertThat(resolved.getUser(), is("crate"));
    }

    /** A reference to something else is not this factory's to answer. */
    @Test
    public void aForeignReferenceIsLeftToItsOwnFactory() throws Exception {
        Reference foreign = new Reference("org.postgresql.ds.PGSimpleDataSource");

        assertThat(new CrateDataSourceFactory()
            .getObjectInstance(foreign, null, null, new Hashtable<>()), is(nullValue()));
    }

    /**
     * A data source assembles its own URL for pgJDBC, which speaks the
     * postgresql scheme.
     */
    @Test
    public void assembledUrlUsesThePostgresqlScheme() {
        CrateDataSource dataSource = new CrateDataSource();
        dataSource.setUrl("crate://crate1.local:5432/doc?loadBalanceHosts=false");

        assertThat(dataSource.getUrl(), startsWith("jdbc:postgresql://crate1.local:5432/doc"));
        assertThat(dataSource.getUrl(), containsString("loadBalanceHosts=false"));
    }
}
