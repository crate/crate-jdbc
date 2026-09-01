package io.crate.client.jdbc.integrationtests;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * A sequence of JDBC calls, written before any of them is made.
 *
 * <p>Generating without executing is what makes a program a thing that can be
 * written down, shrunk and run again. A name enters scope when the step
 * binding it is written, whether or not that step will succeed; a step whose
 * receiver was never bound records that it had nothing to call on, and the
 * program carries on. So the same text is the same program on any driver, and
 * a failure is reproducible from the report rather than from the seed alone.</p>
 */
final class Program {

    /** One call: what to do, what to do it to, and what to call the result. */
    static final class Step {

        private final Verb verb;
        private final String receiver;
        private final List<Object> arguments;
        private final String binds;

        Step(Verb verb, String receiver, List<Object> arguments, String binds) {
            this.verb = verb;
            this.receiver = receiver;
            this.arguments = arguments;
            this.binds = binds;
        }

        Verb verb() {
            return verb;
        }

        String receiver() {
            return receiver;
        }

        List<Object> arguments() {
            return arguments;
        }

        String binds() {
            return binds;
        }

        @Override
        public String toString() {
            StringBuilder rendering = new StringBuilder();
            if (binds != null) {
                rendering.append(binds).append(" = ");
            }
            if (receiver != null) {
                rendering.append(receiver).append('.');
            }
            rendering.append(verb.name().toLowerCase(java.util.Locale.ENGLISH)).append('(');
            for (int i = 0; i < arguments.size(); i++) {
                Object argument = arguments.get(i);
                rendering.append(i > 0 ? ", " : "")
                    .append(argument instanceof String ? "\"" + argument + "\"" : argument);
            }
            return rendering.append(')').toString();
        }
    }

    private final List<Step> steps;

    private Program(List<Step> steps) {
        this.steps = steps;
    }

    List<Step> steps() {
        return steps;
    }

    /**
     * A program of up to the given length, drawn from the seed.
     *
     * <p>A step that may change the rows is followed by a refresh, as a step
     * of its own rather than as something done behind the program's back:
     * CrateDB makes a write readable when it is asked to, so a program that
     * reads what it wrote without asking would read whatever the timing gave
     * it, and one refreshed invisibly could not be replayed from its text.</p>
     */
    static Program of(Random random, int length) {
        List<Step> steps = new ArrayList<>();
        Map<Class<?>, List<String>> scope = new LinkedHashMap<>();
        Map<Class<?>, Integer> named = new LinkedHashMap<>();
        steps.add(bind(Verb.OPEN, null, List.of(), scope, named));
        while (steps.size() < length) {
            List<Verb> callable = new ArrayList<>();
            for (Verb verb : Verb.values()) {
                if (verb != Verb.OPEN && scope.containsKey(verb.receiver())) {
                    callable.add(verb);
                }
            }
            if (callable.isEmpty()) {
                break;
            }
            Verb verb = callable.get(random.nextInt(callable.size()));
            List<String> holders = scope.get(verb.receiver());
            String receiver = holders.get(random.nextInt(holders.size()));
            steps.add(bind(verb, receiver, verb.draw(random, Verb.TABLE), scope, named));
            if (verb.mutates()) {
                steps.add(new Step(Verb.EXECUTE, receiver,
                    List.of("refresh table " + Verb.TABLE), null));
            }
        }
        return new Program(steps);
    }

    private static Step bind(Verb verb, String receiver, List<Object> arguments,
                             Map<Class<?>, List<String>> scope, Map<Class<?>, Integer> named) {
        String binds = null;
        if (verb.binds() != null) {
            int ordinal = named.merge(verb.binds(), 1, Integer::sum) - 1;
            String simple = verb.binds().getSimpleName();
            binds = Character.toLowerCase(simple.charAt(0)) + simple.substring(1) + ordinal;
            scope.computeIfAbsent(verb.binds(), held -> new ArrayList<>()).add(binds);
        }
        return new Step(verb, receiver, arguments, binds);
    }

    /** The same program with a step, and everything that depended on it, left out. */
    Program without(int index) {
        Set<String> lost = new LinkedHashSet<>();
        if (steps.get(index).binds() != null) {
            lost.add(steps.get(index).binds());
        }
        List<Step> kept = new ArrayList<>();
        for (int i = 0; i < steps.size(); i++) {
            Step step = steps.get(i);
            if (i == index) {
                continue;
            }
            if (step.receiver() != null && lost.contains(step.receiver())) {
                if (step.binds() != null) {
                    lost.add(step.binds());
                }
                continue;
            }
            kept.add(step);
        }
        return new Program(kept);
    }

    @Override
    public String toString() {
        StringBuilder rendering = new StringBuilder();
        for (int i = 0; i < steps.size(); i++) {
            rendering.append(String.format("%n  %2d  %s", i + 1, steps.get(i)));
        }
        return rendering.toString();
    }
}
