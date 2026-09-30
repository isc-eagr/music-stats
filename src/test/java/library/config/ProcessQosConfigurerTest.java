package library.config;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessQosConfigurerTest {

    private static final boolean WINDOWS =
            System.getProperty("os.name", "").toLowerCase(Locale.ROOT).startsWith("windows");

    @Test
    void optsTheProcessOutOfExecutionSpeedThrottlingOnWindows() throws Throwable {
        boolean applied = ProcessQosConfigurer.optOutOfPowerThrottling();

        assertThat(applied).isEqualTo(WINDOWS);
        if (WINDOWS) {
            int[] state = readPowerThrottlingState();
            assertThat(state[0] & 0x1).as("ControlMask execution speed").isEqualTo(1);
            assertThat(state[1] & 0x1).as("StateMask execution speed").isZero();
        }
    }

    @Test
    void canBeSwitchedOff() {
        // Must not throw or touch the process when disabled.
        new ProcessQosConfigurer(false).apply();
    }

    private static int[] readPowerThrottlingState() throws Throwable {
        try (Arena arena = Arena.ofConfined()) {
            Linker linker = Linker.nativeLinker();
            SymbolLookup kernel32 = SymbolLookup.libraryLookup("kernel32", arena);
            MethodHandle getCurrentProcess = linker.downcallHandle(
                    kernel32.find("GetCurrentProcess").orElseThrow(), FunctionDescriptor.of(ValueLayout.ADDRESS));
            MethodHandle getProcessInformation = linker.downcallHandle(
                    kernel32.find("GetProcessInformation").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT,
                            ValueLayout.ADDRESS, ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_INT));
            MemorySegment state = arena.allocate(ValueLayout.JAVA_INT, 3);
            state.setAtIndex(ValueLayout.JAVA_INT, 0, 1);
            MemorySegment process = (MemorySegment) getCurrentProcess.invokeExact();
            int ok = (int) getProcessInformation.invokeExact(process, 4, state, (int) state.byteSize());
            assertThat(ok).isNotZero();
            return new int[]{state.getAtIndex(ValueLayout.JAVA_INT, 1), state.getAtIndex(ValueLayout.JAVA_INT, 2)};
        }
    }
}
