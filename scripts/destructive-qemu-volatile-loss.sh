#!/usr/bin/env bash
set -euo pipefail

usage() {
	printf 'usage: %s CONTROL_DIR MELD_LINUX_AMD64 VMLINUZ_VIRT INITRAMFS_VIRT MODLOOP_VIRT\n' "$0" >&2
	exit 2
}

[[ $# -eq 5 ]] || usage
control_dir=$1
meld_binary=$2
kernel=$3
base_initramfs=$4
modloop=$5
script_dir=$(cd "$(dirname "$0")" && pwd -P)
qemu_image='ghcr.io/cross-rs/x86_64-unknown-linux-gnu@sha256:65edeb793308323d185cbfb903778debfff6258c74934ef6a4993ffcdb9763cb'
initramfs_image='alpine@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce'
trial=volatile-loss-1

[[ -d "$control_dir" && -f "$meld_binary" && -f "$kernel" && -f "$base_initramfs" && -f "$modloop" ]] || usage
control_dir=$(cd "$control_dir" && pwd -P)
cleanup() { rm -f "$control_dir/qmp.sock"; }
trap cleanup EXIT INT TERM

cp "$meld_binary" "$control_dir/meld"
cp "$kernel" "$control_dir/vmlinuz-virt"
cp "$base_initramfs" "$control_dir/initramfs-virt"
cp "$modloop" "$control_dir/modloop-virt"
cp "$script_dir/destructive-qemu-volatile-loss-guest-init.sh" "$control_dir/volatile-loss-guest-init.sh"
chmod 0755 "$control_dir/meld" "$control_dir/volatile-loss-guest-init.sh"

docker run --rm -v "$control_dir:/control" "$initramfs_image" sh -lc '
	set -eu
	rm -rf /tmp/initrd
	mkdir /tmp/initrd
	chmod 0755 /tmp/initrd
	cd /tmp/initrd
	zcat /control/initramfs-virt | cpio -idmu >/dev/null 2>&1
	cp /control/volatile-loss-guest-init.sh init
	chmod 0755 . init
	find . -print | cpio -o -H newc 2>/dev/null | gzip -9 > /control/initramfs-volatile-loss
'

rm -rf "$control_dir/volatile-loss-root"
mkdir "$control_dir/volatile-loss-root"
rm -f "$control_dir/$trial-"* "$control_dir/volatile-loss-seed.json" "$control_dir/target-volatile-loss.img" "$control_dir/qmp.sock"
docker run --rm -v "$control_dir:/control" "$qemu_image" sh -lc '
	set -eu
	/control/meld destructive-volatile-loss-seed --database /control/volatile-loss-root/power.meld > /control/volatile-loss-seed.json
	chown 65534:65534 /control/volatile-loss-root/power.meld
	truncate -s 128M /control/target-volatile-loss.img
	mkfs.ext4 -q -F -d /control/volatile-loss-root /control/target-volatile-loss.img
'

qemu_args=(
	-machine accel=tcg -cpu max -m 512 -display none -serial "file:/control/$trial-qemu.log"
	-kernel /control/vmlinuz-virt -initrd /control/initramfs-volatile-loss
	-append "console=ttyS0 panic=-1 meld_trial=$trial"
	-snapshot
	-drive file=/control/target-volatile-loss.img,if=virtio,format=raw,cache=none,aio=threads
	-drive file=/control/modloop-virt,if=virtio,format=raw,readonly=on
	-virtfs local,path=/control,mount_tag=control,security_model=none,multidevs=remap
	-qmp unix:/control/qmp.sock,server=on,wait=off
)

docker run --rm --privileged -v "$control_dir:/control" "$qemu_image" \
	/control/meld destructive-qemu-volatile-loss \
	--marker "/control/$trial-marker.json" --recovery-ready "/control/$trial-recovery-ready.json" \
	--result "/control/$trial-result.json" --qmp-socket /control/qmp.sock \
	--proof "/control/$trial-proof.json" --event "/control/$trial-controller.json" \
	--qemu-log "/control/$trial-qemu-process.log" --target-image /control/target-volatile-loss.img \
	--base-artifact "/control/$trial-base-after-kill.img" --artifact-uid 65534 --artifact-gid 65534 \
	--timeout 5m -- /usr/local/bin/qemu-system-x86_64 "${qemu_args[@]}" \
	>"$control_dir/$trial-controller.stdout.json"

jq -e '.acknowledgedCommitLost == true and .monotonicFloorRejected == true and .unsafeStorageDetected == true and .negativeControlPassed == true and .acknowledgedCommitSequence == 2 and .recoveredCommitSequence == 1' \
	"$control_dir/$trial-result.json" >/dev/null
docker run --rm -v "$control_dir:/control" "$qemu_image" /control/meld destructive-qemu-volatile-loss-proof-check \
	--proof "/control/$trial-proof.json" --marker "/control/$trial-marker.json" \
	--recovery-ready "/control/$trial-recovery-ready.json" >"$control_dir/$trial-proof-check.json"
docker run --rm -v "$control_dir:/control" "$qemu_image" /control/meld destructive-volatile-loss-bundle-check \
	--seed /control/volatile-loss-seed.json --marker "/control/$trial-marker.json" \
	--recovery-ready "/control/$trial-recovery-ready.json" --proof "/control/$trial-proof.json" \
	--event "/control/$trial-controller.json" --result "/control/$trial-result.json" \
	>"$control_dir/$trial-bundle-check.json"
rm -f "$control_dir/target-volatile-loss.img"
printf 'volatile-loss negative control detected acknowledged sequence 2 rollback to 1 proof=%s\n' \
	"$(sed -n 's/.*"proofSha256":"\([0-9a-f]*\)".*/\1/p' "$control_dir/$trial-proof-check.json")"
