package gpu

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------------------------------
// adaptiveCombinationCap
// ----------------------------------------------------------------------------

func TestAdaptiveCombinationCap_ExhaustiveWhenSmall(t *testing.T) {
	// C(8, 4) = 70 — way under the 50k hard cap, should enumerate exhaustively.
	assert.Equal(t, 70, adaptiveCombinationCap(8, 4))

	// C(4, 2) = 6.
	assert.Equal(t, 6, adaptiveCombinationCap(4, 2))

	// Single-GPU single-request (edge case).
	assert.Equal(t, 1, adaptiveCombinationCap(1, 1))
}

func TestAdaptiveCombinationCap_SamplesWhenLarge(t *testing.T) {
	// C(64, 8) ≈ 4.4 billion — far over the hard cap, should clamp.
	assert.Equal(t, 50_000, adaptiveCombinationCap(64, 8))

	// C(100, 50) overflows int64 on 32-bit; overflow branch must still clamp.
	assert.Equal(t, 50_000, adaptiveCombinationCap(100, 50))
}

func TestAdaptiveCombinationCap_DegenerateInputs(t *testing.T) {
	assert.Equal(t, 0, adaptiveCombinationCap(0, 0))
	assert.Equal(t, 0, adaptiveCombinationCap(5, 0))
	assert.Equal(t, 0, adaptiveCombinationCap(0, 3))
	assert.Equal(t, 0, adaptiveCombinationCap(3, 5)) // k > n
	assert.Equal(t, 0, adaptiveCombinationCap(-1, 2))
}

func TestBinomial_KnownValues(t *testing.T) {
	assert.Equal(t, 1, binomial(5, 0))
	assert.Equal(t, 5, binomial(5, 1))
	assert.Equal(t, 10, binomial(5, 2))
	assert.Equal(t, 10, binomial(5, 3))
	assert.Equal(t, 1, binomial(5, 5))
	assert.Equal(t, 70, binomial(8, 4))
	assert.Equal(t, 0, binomial(3, 4))
}

// ----------------------------------------------------------------------------
// sysfs-first NUMA mapping
// ----------------------------------------------------------------------------

func TestQueryGPUNUMAMappingSysfs_SingleGPU(t *testing.T) {
	root := t.TempDir()
	writeFakeNVIDIADevice(t, root, "0000:01:00.0", "0")

	withSysfsRoot(t, root)

	gtm := &GPUTopologyManager{}
	mapping, ok := gtm.queryGPUNUMAMappingSysfs()
	require.True(t, ok, "sysfs enumeration should succeed with one device")
	assert.Equal(t, map[int]int{0: 0}, mapping)
}

func TestQueryGPUNUMAMappingSysfs_MultipleGPUsSortedByBusID(t *testing.T) {
	root := t.TempDir()
	// Intentionally create in reverse order to verify we sort by bus ID.
	writeFakeNVIDIADevice(t, root, "0000:81:00.0", "1")
	writeFakeNVIDIADevice(t, root, "0000:01:00.0", "0")

	withSysfsRoot(t, root)

	gtm := &GPUTopologyManager{}
	mapping, ok := gtm.queryGPUNUMAMappingSysfs()
	require.True(t, ok)
	// Sorted: 0000:01:00.0 (idx 0, NUMA 0), 0000:81:00.0 (idx 1, NUMA 1)
	assert.Equal(t, map[int]int{0: 0, 1: 1}, mapping)
}

func TestQueryGPUNUMAMappingSysfs_NegativeNUMANormalisedToZero(t *testing.T) {
	// Laptops / VMs often report numa_node = -1 (unknown). We coerce to 0
	// so downstream scheduling isn't handed a negative index.
	root := t.TempDir()
	writeFakeNVIDIADevice(t, root, "0000:01:00.0", "-1")

	withSysfsRoot(t, root)

	gtm := &GPUTopologyManager{}
	mapping, ok := gtm.queryGPUNUMAMappingSysfs()
	require.True(t, ok)
	assert.Equal(t, map[int]int{0: 0}, mapping)
}

func TestQueryGPUNUMAMappingSysfs_SkipsNonPCIEntries(t *testing.T) {
	root := t.TempDir()
	writeFakeNVIDIADevice(t, root, "0000:01:00.0", "0")
	// Driver dir normally contains pseudo-files like "bind", "module", etc.
	require.NoError(t, os.MkdirAll(filepath.Join(root, "bus/pci/drivers/nvidia/module"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "bus/pci/drivers/nvidia/bind"), []byte(""), 0o644))

	withSysfsRoot(t, root)

	gtm := &GPUTopologyManager{}
	mapping, ok := gtm.queryGPUNUMAMappingSysfs()
	require.True(t, ok)
	assert.Equal(t, map[int]int{0: 0}, mapping)
}

func TestQueryGPUNUMAMappingSysfs_NoDriverDir(t *testing.T) {
	// If /sys/bus/pci/drivers/nvidia doesn't exist (e.g. driver not loaded),
	// the sysfs path reports ok=false so the caller falls back to nvidia-smi.
	withSysfsRoot(t, t.TempDir())

	gtm := &GPUTopologyManager{}
	_, ok := gtm.queryGPUNUMAMappingSysfs()
	assert.False(t, ok)
}

func TestLooksLikePCIAddress(t *testing.T) {
	cases := map[string]bool{
		"0000:01:00.0": true,
		"0000:81:1f.7": true,
		"abcd:ef:01.0": true,
		"bind":         false,
		"unbind":       false,
		"module":       false,
		"0000:01:00":   false, // too short
		"0000-01:00.0": false, // wrong separator
	}
	for in, expected := range cases {
		assert.Equalf(t, expected, looksLikePCIAddress(in), "input=%q", in)
	}
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

// writeFakeNVIDIADevice lays out a minimal sysfs fixture for one NVIDIA GPU:
//
//	<root>/bus/pci/drivers/nvidia/<busID>       (a directory whose name is the bus ID)
//	<root>/bus/pci/devices/<busID>/numa_node    (contains the NUMA node as text)
func writeFakeNVIDIADevice(t *testing.T, root, busID, numaNode string) {
	t.Helper()
	driverDir := filepath.Join(root, "bus/pci/drivers/nvidia", busID)
	require.NoError(t, os.MkdirAll(driverDir, 0o755))

	devDir := filepath.Join(root, "bus/pci/devices", busID)
	require.NoError(t, os.MkdirAll(devDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(devDir, "numa_node"), []byte(numaNode+"\n"), 0o644))
}

// withSysfsRoot swaps sysfsRoot for the duration of a test and restores it
// via t.Cleanup — keeps the production default intact for non-test code paths.
func withSysfsRoot(t *testing.T, root string) {
	t.Helper()
	orig := sysfsRoot
	sysfsRoot = root
	t.Cleanup(func() { sysfsRoot = orig })
}
