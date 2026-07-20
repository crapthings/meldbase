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
container="meld-qemu-flush-eio-$$"
recovery_container="meld-qemu-flush-eio-recovery-$$"
trial=flush-eio-1

[[ -d "$control_dir" && -f "$meld_binary" && -f "$kernel" && -f "$base_initramfs" && -f "$modloop" ]] || usage
control_dir=$(cd "$control_dir" && pwd -P)
cleanup() {
	docker rm -f "$container" >/dev/null 2>&1 || true
	docker rm -f "$recovery_container" >/dev/null 2>&1 || true
	rm -f "$control_dir/qmp.sock"
}
trap cleanup EXIT INT TERM

cp "$meld_binary" "$control_dir/meld"
cp "$kernel" "$control_dir/vmlinuz-virt"
cp "$base_initramfs" "$control_dir/initramfs-virt"
cp "$modloop" "$control_dir/modloop-virt"
cp "$script_dir/destructive-qemu-flush-eio-guest-init.sh" "$control_dir/flush-eio-guest-init.sh"
chmod 0755 "$control_dir/meld" "$control_dir/flush-eio-guest-init.sh"

docker run --rm -v "$control_dir:/control" "$initramfs_image" sh -lc '
	set -eu
	rm -rf /tmp/initrd
	mkdir /tmp/initrd
	chmod 0755 /tmp/initrd
	cd /tmp/initrd
	zcat /control/initramfs-virt | cpio -idmu >/dev/null 2>&1
	cp /control/flush-eio-guest-init.sh init
	chmod 0755 . init
	find . -print | cpio -o -H newc 2>/dev/null | gzip -9 > /control/initramfs-flush-eio
'

rm -rf "$control_dir/flush-eio-root"
mkdir "$control_dir/flush-eio-root"
rm -f "$control_dir/$trial-"* "$control_dir/target-flush-eio.img" "$control_dir/qmp.sock"
docker run --rm -v "$control_dir:/control" "$qemu_image" sh -lc '
	set -eu
	/control/meld destructive-eio-seed --database /control/flush-eio-root/source.meld > /control/flush-eio-seed.json
	chown 65534:65534 /control/flush-eio-root/source.meld
	truncate -s 128M /control/target-flush-eio.img
	mkfs.ext4 -q -F -d /control/flush-eio-root /control/target-flush-eio.img
'

docker run -d --name "$container" --privileged -v "$control_dir:/control" "$qemu_image" \
	/usr/local/bin/qemu-system-x86_64 \
	-machine accel=tcg -cpu max -m 512 -display none -serial "file:/control/$trial-qemu.log" \
	-kernel /control/vmlinuz-virt -initrd /control/initramfs-flush-eio \
	-append "console=ttyS0 panic=-1 meld_trial=$trial meld_phase=fault" \
	-blockdev driver=file,filename=/control/target-flush-eio.img,node-name=meld-file,cache.direct=on,cache.no-flush=off \
	-blockdev driver=blkdebug,image=meld-file,node-name=meld-debug \
	-blockdev driver=raw,file=meld-debug,node-name=meld-raw \
	-device virtio-blk-pci,drive=meld-raw,write-cache=on \
	-drive file=/control/modloop-virt,if=virtio,format=raw,readonly=on \
	-virtfs local,path=/control,mount_tag=control,security_model=none,multidevs=remap \
	-qmp unix:/control/qmp.sock,server=on,wait=off >/dev/null

for ignored in $(seq 1 200); do
	[[ -S "$control_dir/qmp.sock" ]] && break
	sleep 0.1
done
[[ -S "$control_dir/qmp.sock" ]]

docker run --rm -v "$control_dir:/control" "$qemu_image" /control/meld destructive-qemu-flush-eio \
	--ready "/control/$trial-ready.json" --armed "/control/$trial-armed.json" \
	--fault "/control/$trial-fault.json" --qmp-socket /control/qmp.sock \
	--proof "/control/$trial-proof.json" --ack "/control/$trial-ack.json" \
	--target-image /control/target-flush-eio.img --timeout 3m \
	>"$control_dir/$trial-controller.stdout.json"

for ignored in $(seq 1 600); do
	state=$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || true)
	[[ "$state" == false ]] && break
	sleep 0.1
done
docker wait "$container" >/dev/null
docker rm "$container" >/dev/null

docker run --rm -v "$control_dir:/control" "$qemu_image" /control/meld destructive-flush-eio-recovery-plan \
	--target-image /control/target-flush-eio.img --fault "/control/$trial-fault.json" \
	--proof "/control/$trial-proof.json" --out "/control/$trial-recovery-plan.json" \
	>"$control_dir/$trial-recovery-plan.stdout.json"

docker run -d --name "$recovery_container" --privileged -v "$control_dir:/control" "$qemu_image" \
	/usr/local/bin/qemu-system-x86_64 \
	-machine accel=tcg -cpu max -m 512 -display none -serial "file:/control/$trial-recovery-qemu.log" \
	-kernel /control/vmlinuz-virt -initrd /control/initramfs-flush-eio \
	-append "console=ttyS0 panic=-1 meld_trial=$trial meld_phase=recovery" \
	-blockdev driver=file,filename=/control/target-flush-eio.img,node-name=meld-recovery-file,cache.direct=on,cache.no-flush=off \
	-blockdev driver=raw,file=meld-recovery-file,node-name=meld-recovery-raw \
	-device virtio-blk-pci,drive=meld-recovery-raw,write-cache=on \
	-drive file=/control/modloop-virt,if=virtio,format=raw,readonly=on \
	-virtfs local,path=/control,mount_tag=control,security_model=none,multidevs=remap >/dev/null

for ignored in $(seq 1 600); do
	state=$(docker inspect -f '{{.State.Running}}' "$recovery_container" 2>/dev/null || true)
	[[ "$state" == false ]] && break
	sleep 0.1
done
docker wait "$recovery_container" >/dev/null
docker rm "$recovery_container" >/dev/null

docker run --rm -v "$control_dir:/control" "$qemu_image" /control/meld destructive-flush-eio-result-check \
	--result "/control/$trial-result.json" >"$control_dir/$trial-result-check.json"
docker run --rm -v "$control_dir:/control" "$qemu_image" /control/meld destructive-qemu-flush-eio-proof-check \
	--proof "/control/$trial-proof.json" --ready "/control/$trial-ready.json" --armed "/control/$trial-armed.json" \
	--fault "/control/$trial-fault.json" >"$control_dir/$trial-proof-check.json"
docker run --rm -v "$control_dir:/control" "$qemu_image" /control/meld destructive-flush-eio-bundle-check \
	--seed /control/flush-eio-seed.json --ready "/control/$trial-ready.json" --armed "/control/$trial-armed.json" \
	--fault "/control/$trial-fault.json" --proof "/control/$trial-proof.json" \
	--recovery-plan "/control/$trial-recovery-plan.json" --recovery-ready "/control/$trial-recovery-ready.json" \
	--result "/control/$trial-result.json" >"$control_dir/$trial-bundle-check.json"
printf 'flush-eio verified result=%s proof=%s\n' \
	"$(sed -n 's/.*"resultSha256":"\([0-9a-f]*\)".*/\1/p' "$control_dir/$trial-result-check.json")" \
	"$(sed -n 's/.*"proofSha256":"\([0-9a-f]*\)".*/\1/p' "$control_dir/$trial-proof-check.json")"
