package io.crate.client.jdbc.integrationtests;

/**
 * The number every drawn value and every generated program comes from.
 *
 * <p>Pinned unless {@code -PtestSeed} says otherwise, so a run on a pull
 * request asks the same questions as the last one and a failure is the change
 * rather than the draw. A run that is looking for new faults rather than
 * guarding old ones passes a seed of its own, and the seed is printed either
 * way — a finding is worth nothing if the run that found it cannot be had
 * again.</p>
 */
final class DeviceSeed {

    private static final long PINNED = 20_260_805L;

    private static boolean announced;

    private DeviceSeed() {
    }

    static synchronized long value() {
        String chosen = System.getProperty("test.seed");
        long seed = chosen == null || chosen.trim().isEmpty() ? PINNED : Long.parseLong(chosen.trim());
        if (!announced) {
            announced = true;
            System.out.println("Generated values and programs come from seed " + seed
                + " (-PtestSeed=" + seed + " asks the same questions again)");
        }
        return seed;
    }
}
