package library.config;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.Locale;

/**
 * Opts the app's process out of Windows power throttling (EcoQoS / "efficiency mode").
 * The app usually runs without a visible window, so on hybrid CPUs Windows can treat it as
 * background work and run every request on efficiency cores at reduced clocks, which made
 * page loads roughly 2x slower. Disable with --musicstats.performance.high-qos=false.
 */
@Component
public class ProcessQosConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(ProcessQosConfigurer.class);

    // PROCESS_INFORMATION_CLASS.ProcessPowerThrottling and PROCESS_POWER_THROTTLING_STATE values.
    private static final int PROCESS_POWER_THROTTLING = 4;
    private static final int PROCESS_POWER_THROTTLING_CURRENT_VERSION = 1;
    private static final int PROCESS_POWER_THROTTLING_EXECUTION_SPEED = 0x1;

    private final boolean enabled;

    public ProcessQosConfigurer(@Value("${musicstats.performance.high-qos:true}") boolean enabled) {
        this.enabled = enabled;
    }

    @PostConstruct
    public void apply() {
        if (!enabled) {
            return;
        }
        if (optOutOfPowerThrottling()) {
            logger.info("Windows power throttling disabled for this process (high QoS).");
        }
    }

    /** Returns true when Windows accepted the request; false on other platforms or on failure. */
    static boolean optOutOfPowerThrottling() {
        if (!System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows")) {
            return false;
        }
        try (Arena arena = Arena.ofConfined()) {
            Linker linker = Linker.nativeLinker();
            SymbolLookup kernel32 = SymbolLookup.libraryLookup("kernel32", arena);
            MethodHandle getCurrentProcess = linker.downcallHandle(
                    kernel32.find("GetCurrentProcess").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.ADDRESS));
            MethodHandle setProcessInformation = linker.downcallHandle(
                    kernel32.find("SetProcessInformation").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT,
                            ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));

            MemorySegment state = arena.allocate(ValueLayout.JAVA_INT, 3);
            state.setAtIndex(ValueLayout.JAVA_INT, 0, PROCESS_POWER_THROTTLING_CURRENT_VERSION);
            state.setAtIndex(ValueLayout.JAVA_INT, 1, PROCESS_POWER_THROTTLING_EXECUTION_SPEED); // ControlMask
            state.setAtIndex(ValueLayout.JAVA_INT, 2, 0); // StateMask 0 = never throttle execution speed

            MemorySegment process = (MemorySegment) getCurrentProcess.invokeExact();
            int ok = (int) setProcessInformation.invokeExact(process, PROCESS_POWER_THROTTLING, state, (int) state.byteSize());
            return ok != 0;
        } catch (Throwable e) {
            logger.info("Could not opt out of Windows power throttling: {}", e.toString());
            return false;
        }
    }
}
