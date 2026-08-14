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

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * Emits the Forwarding* delegate base classes in
 * driver/main/java/io/crate/client/jdbc/: one abstract class per wrapped
 * java.sql interface, forwarding every method to a delegate instance.
 * Behavioral overrides live in the hand-written Crate* subclasses, so the
 * generated files carry no logic and can be regenerated wholesale whenever
 * a newer JDBC spec adds methods. Where java.sql nests one interface inside
 * another, the generated class picks up where the inner interface's wrapper
 * leaves off, so each level is generated plumbing over the behavior below it
 * and no behavior is written twice.
 *
 * <p>Regenerate with {@code make forwarding}, which runs
 *
 * <pre>
 *   java -cp path/to/postgresql.jar devtools/GenerateForwarding.java [output dir]
 * </pre>
 *
 * from the repository root, against the pgJDBC and the JDK the build pins.
 * pgJDBC has to be on the classpath because the connection and statement
 * wrappers carry its public interfaces too. Launching it directly picks up
 * whichever JDK is on PATH, and the generated sources compile against the older
 * Java version in {@code options.release}: methods java.sql has gained since
 * then are listed in {@link #NOT_IN_RELEASE_11}, and which of those apply is a
 * property of the generating JDK.</p>
 */
public class GenerateForwarding {

    /**
     * The header every source file in this project carries, read from the one
     * place it is written so that Spotless and the generator cannot disagree
     * about it.
     */
    private static final Path LICENSE_HEADER_FILE = Path.of("gradle", "license-header.txt");

    /**
     * The JDBC interface a Forwarding* class wraps and the field holding the
     * delegate at that type. Where java.sql nests one interface inside another,
     * it also carries the wrapper this one extends and the interface that
     * wrapper already answers. The extra pgJDBC interfaces keep the
     * pgJDBC-specific API reachable by a plain cast, as it is written in the
     * wild: {@code ((PGConnection) connection).getNotifications()}.
     */
    private record Spec(Class<?> iface, String field, Class<?> covered, String base,
                        Class<?>... extraInterfaces) {

        /** A wrapper over a delegate of its own, extending nothing. */
        static Spec of(Class<?> iface, Class<?>... extraInterfaces) {
            return new Spec(iface, "delegate", null, null, extraInterfaces);
        }
    }

    private static final Spec[] SPECS = {
        Spec.of(java.sql.Connection.class, org.postgresql.PGConnection.class),
        Spec.of(java.sql.Statement.class, org.postgresql.PGStatement.class),
        // A prepared statement is a statement whose text is fixed and whose
        // parameters are bound; a call is a prepared statement whose parameters
        // can also be addressed by name. Each wrapper starts from the one below
        // it and forwards only what its own interface adds, so a statement's
        // handling of query timeouts and result sets is written once and
        // inherited all the way down.
        new Spec(java.sql.PreparedStatement.class, "preparedDelegate",
            java.sql.Statement.class, "CrateStatement"),
        new Spec(java.sql.CallableStatement.class, "callableDelegate",
            java.sql.PreparedStatement.class, "CratePreparedStatement"),
        Spec.of(java.sql.ResultSet.class),
        Spec.of(java.sql.DatabaseMetaData.class),
        Spec.of(java.sql.ResultSetMetaData.class, org.postgresql.PGResultSetMetaData.class),
        Spec.of(java.sql.ParameterMetaData.class),
    };

    private static Spec specOf(Class<?> iface) {
        for (Spec spec : SPECS) {
            if (spec.iface() == iface) {
                return spec;
            }
        }
        throw new IllegalArgumentException("No spec for " + iface);
    }

    /**
     * Methods present in the generating JDK's java.sql but absent from the
     * Java 11 API this project compiles against (--release 11). Keyed as
     * "DeclaringInterfaceSimpleName#methodName". A newer generating JDK can
     * add entries here; the compiler names whatever is missing.
     *
     * <p>A method left out is not forwarded, so a wrapper answers it with
     * whatever default {@code java.sql} gives the interface. That is all a
     * driver built for Java 11 can do about a method Java 11 does not have.</p>
     *
     * <p>An entry naming a method the generating JDK does not declare matches
     * nothing and costs nothing: the four below are on {@code Statement}
     * through Java 21, and on {@code Connection} as well from Java 25. Whether
     * an entry applies is therefore a property of the generating JDK, and the
     * build pins it so that regenerating produces the same sources everywhere.
     * A method a newer JDK adds without an entry here is emitted, and the
     * {@code --release 11} compile names it.</p>
     */
    private static final java.util.Set<String> NOT_IN_RELEASE_11 = java.util.Set.of(
        "Connection#enquoteIdentifier",
        "Connection#enquoteLiteral",
        "Connection#enquoteNCharLiteral",
        "Connection#isSimpleIdentifier"
    );

    private static String licenseHeader;

    public static void main(String[] args) throws Exception {
        Path outDir = args.length > 0
            ? Path.of(args[0])
            : Path.of("driver/main/java/io/crate/client/jdbc");
        licenseHeader = Files.readString(LICENSE_HEADER_FILE);
        Files.createDirectories(outDir);
        for (Spec spec : SPECS) {
            String className = "Forwarding" + spec.iface().getSimpleName();
            Files.writeString(outDir.resolve(className + ".java"), render(spec, className));
            System.out.println("wrote " + outDir.resolve(className + ".java"));
        }
    }

    private static String render(Spec spec, String className) {
        Class<?> iface = spec.iface();
        Class<?>[] extras = spec.extraInterfaces();
        String base = spec.base();
        String pgField = "pgDelegate";
        StringBuilder sb = new StringBuilder();
        sb.append(licenseHeader)
          .append("package io.crate.client.jdbc;\n\n")
          .append("import ").append(iface.getName()).append(";\n");
        for (Class<?> extra : extras) {
            sb.append("import ").append(extra.getName()).append(";\n");
        }
        boolean throwsSqlException = extras.length > 0 || base != null;
        if (throwsSqlException) {
            sb.append("import java.sql.SQLException;\n");
        }
        sb.append("\n")
          .append("/**\n")
          .append(" * Forwards every {@link ").append(iface.getSimpleName())
          .append("} method ").append(base == null ? "to a delegate" : base + " does not answer")
          .append(".\n")
          .append(" * Generated by devtools/GenerateForwarding.java — do not edit by hand.\n")
          .append(" */\n")
          // A wrapper owes an answer to every method of the interface,
          // including the ones JDBC has deprecated.
          .append("@SuppressWarnings(\"deprecation\")\n")
          .append("public abstract class ").append(className);
        if (base != null) {
            sb.append(" extends ").append(base);
        }
        sb.append(" implements ").append(iface.getSimpleName());
        for (Class<?> extra : extras) {
            sb.append(", ").append(extra.getSimpleName());
        }
        sb.append(" {\n\n")
          .append("    protected final ").append(iface.getSimpleName()).append(" ")
          .append(spec.field()).append(";\n");
        for (Class<?> extra : extras) {
            sb.append("    protected final ").append(extra.getSimpleName()).append(" ")
              .append(pgField).append(";\n");
        }
        sb.append("\n    protected ").append(className).append("(")
          .append(iface.getSimpleName()).append(" delegate");
        if (base != null) {
            sb.append(", CrateConnection connection");
        }
        sb.append(")");
        if (throwsSqlException) {
            sb.append(" throws SQLException");
        }
        sb.append(" {\n");
        if (base != null) {
            sb.append("        super(delegate, connection);\n");
        }
        sb.append("        this.").append(spec.field()).append(" = delegate;\n");
        for (Class<?> extra : extras) {
            sb.append("        this.").append(pgField).append(" = delegate.unwrap(")
              .append(extra.getSimpleName()).append(".class);\n");
        }
        sb.append("    }\n");
        if (spec.covered() == null) {
            sb.append("\n    @Override\n")
              .append("    public <T> T unwrap(java.lang.Class<T> p0) throws java.sql.SQLException {\n")
              .append("        return p0.isInstance(this) ? p0.cast(this) : delegate.unwrap(p0);\n")
              .append("    }\n")
              .append("\n    @Override\n")
              .append("    public boolean isWrapperFor(java.lang.Class<?> p0) throws java.sql.SQLException {\n")
              .append("        return p0.isInstance(this) || delegate.isWrapperFor(p0);\n")
              .append("    }\n");
        }

        Map<String, Emit> methods = new LinkedHashMap<>();
        inherited(methods, spec.covered());
        collect(methods, iface, spec.field());
        for (Class<?> extra : extras) {
            collect(methods, extra, pgField);
        }
        methods.values().removeIf(emit -> emit.target() == null);
        List<Emit> sorted = new ArrayList<>(methods.values());
        sorted.sort(Comparator.comparing((Emit e) -> e.method().getName())
            .thenComparing(e -> e.method().getParameterCount())
            .thenComparing(e -> e.method().toString()));

        for (Emit emit : sorted) {
            Method m = emit.method();
            sb.append("\n    @Override\n    public ");
            TypeVariable<?>[] typeParams = m.getTypeParameters();
            if (typeParams.length > 0) {
                StringJoiner tj = new StringJoiner(", ", "<", "> ");
                for (TypeVariable<?> tv : typeParams) {
                    tj.add(tv.getName());
                }
                sb.append(tj);
            }
            sb.append(typeName(m.getGenericReturnType())).append(" ").append(m.getName()).append("(");
            Type[] params = m.getGenericParameterTypes();
            StringJoiner pj = new StringJoiner(", ");
            for (int i = 0; i < params.length; i++) {
                pj.add(typeName(params[i]) + " p" + i);
            }
            sb.append(pj).append(")");
            Type[] exceptions = m.getGenericExceptionTypes();
            if (exceptions.length > 0) {
                StringJoiner ej = new StringJoiner(", ", " throws ", "");
                for (Type e : exceptions) {
                    ej.add(typeName(e));
                }
                sb.append(ej);
            }
            sb.append(" {\n        ");
            if (m.getReturnType() != void.class) {
                sb.append("return ");
            }
            sb.append(emit.target()).append(".").append(m.getName()).append("(");
            StringJoiner aj = new StringJoiner(", ");
            for (int i = 0; i < params.length; i++) {
                aj.add("p" + i);
            }
            sb.append(aj).append(");\n    }\n");
        }
        sb.append("}\n");
        return sb.toString();
    }

    private record Emit(Method method, String target) {
    }

    /**
     * Claims the methods an inherited wrapper already answers, with no target,
     * so that {@link #collect} passes over them and they are dropped before
     * anything is emitted.
     */
    private static void inherited(Map<String, Emit> methods, Class<?> covered) {
        if (covered == null) {
            return;
        }
        Spec spec = specOf(covered);
        inherited(methods, spec.covered());
        collect(methods, covered, null);
        for (Class<?> extra : spec.extraInterfaces()) {
            collect(methods, extra, null);
        }
    }

    /**
     * Adds every instance method of {@code source} that is not already
     * covered, forwarding it to {@code target}. Methods the JDBC spec
     * implements for wrappers are handled outside the generated forwarding.
     */
    private static void collect(Map<String, Emit> methods, Class<?> source, String target) {
        for (Method m : source.getMethods()) {
            if (Modifier.isStatic(m.getModifiers())) {
                continue;
            }
            if (m.getName().equals("unwrap") || m.getName().equals("isWrapperFor")) {
                continue;
            }
            if (NOT_IN_RELEASE_11.contains(m.getDeclaringClass().getSimpleName() + "#" + m.getName())) {
                continue;
            }
            String key = m.getName() + java.util.Arrays.stream(m.getParameterTypes())
                .map(Class::getName).collect(Collectors.joining(","));
            methods.putIfAbsent(key, new Emit(m, target));
        }
    }

    private static String typeName(Type type) {
        if (type instanceof Class<?> cls) {
            return cls.getCanonicalName();
        }
        if (type instanceof ParameterizedType pt) {
            StringJoiner tj = new StringJoiner(", ", "<", ">");
            for (Type arg : pt.getActualTypeArguments()) {
                tj.add(typeName(arg));
            }
            return typeName(pt.getRawType()) + tj;
        }
        if (type instanceof GenericArrayType gat) {
            return typeName(gat.getGenericComponentType()) + "[]";
        }
        if (type instanceof TypeVariable<?> tv) {
            return tv.getName();
        }
        if (type instanceof WildcardType wt) {
            Type[] upper = wt.getUpperBounds();
            Type[] lower = wt.getLowerBounds();
            if (lower.length > 0) {
                return "? super " + typeName(lower[0]);
            }
            if (upper.length == 1 && upper[0] == Object.class) {
                return "?";
            }
            return "? extends " + typeName(upper[0]);
        }
        throw new IllegalArgumentException("Cannot render type: " + type);
    }
}
