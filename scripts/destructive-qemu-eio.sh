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
container="meld-qemu-eio-$$"
trial=eio-write-aio-1

[[ -d "$control_dir" && -f "$meld_binary" && -f "$kernel" && -f "$base_initramfs" && -f "$modloop" ]] || usage
control_dir=$(cd "$control_dir" && pwd -P)

cleanup() {
	docker rm -f "$container" >/dev/null 2>&1 || true
	rm -f "$control_dir/qmp.sock"
}
trap cleanup EXIT INT TERM

cp "$meld_binary" "$control_dir/meld"
cp "$kernel" "$control_dir/vmlinuz-virt"
cp "$base_initramfs" "$control_dir/initramfs-virt"
cp "$modloop" "$control_dir/modloop-virt"
cp "$script_dir/destructive-qemu-eio-guest-init.sh" "$control_dir/eio-guest-init.sh"
chmod 0755 "$control_dir/meld" "$control_dir/eio-guest-init.sh"

docker run --rm -v "$control_dir:/control" "$initramfs_image" sh -lc '
	set -eu
	rm -rf /tmp/initrd
	mkdir /tmp/initrd
	chmod 0755 /tmp/initrd
	cd /tmp/initrd
	zcat /control/initramfs-virt | cpio -idmu >/dev/null 2>&1
	cp /control/eio-guest-init.sh init
	chmod 0755 . init
	find . -print | cpio -o -H newc 2>/dev/null | gzip -9 > /control/initramfs-eio
'

rm -rf "$control_dir/eio-root"
mkdir "$control_dir/eio-root"
rm -f "$control_dir/$trial-"* "$control_dir/target-eio.img" "$control_dir/blkdebug-eio.conf" "$control_dir/qmp.sock"

docker run --rm -v "$control_dir:/control" "$qemu_image" sh -lc '
	set -eu
	/control/meld destructive-eio-seed --database /control/eio-root/source.meld > /control/eio-seed.json
	chown 65534:65534 /control/eio-root/source.meld
	truncate -s 128M /control/target-eio.img
	mkfs.ext4 -q -F -d /control/eio-root /control/target-eio.img
	block_size=$(dumpe2fs -h /control/target-eio.img 2>/dev/null | awk -F: "/Block size:/ {gsub(/ /, \"\", \$2); print \$2}")
	blocks=$(debugfs -R "blocks /source.meld" /control/target-eio.img 2>/dev/null)
	printf "%s\n" "$block_size" > /control/eio-block-size.txt
	printf "%s\n" "$blocks" > /control/eio-blocks.txt
	: > /control/blkdebug-eio.conf
	for block in $blocks; do
		first=$((block * block_size / 512))
		last=$(((block + 1) * block_size / 512 - 1))
		for sector in $(seq "$first" "$last"); do
			printf "[inject-error]\nevent = \"write_aio\"\niotype = \"write\"\nerrno = \"5\"\nsector = \"%s\"\nonce = \"on\"\n\n" "$sector" >> /control/blkdebug-eio.conf
		done
	done
	test -s /control/blkdebug-eio.conf
'

docker run -d --name "$container" --privileged -v "$control_dir:/control" "$qemu_image" \
	/usr/local/bin/qemu-system-x86_64 \
	-machine accel=tcg -cpu max -m 512 -display none -serial "file:/control/$trial-qemu.log" \
	-kernel /control/vmlinuz-virt -initrd /control/initramfs-eio \
	-append "console=ttyS0 panic=-1 meld_trial=$trial" \
	-drive "file=blkdebug:/control/blkdebug-eio.conf:/control/target-eio.img,if=virtio,format=raw,cache=none,aio=threads" \
	-drive file=/control/modloop-virt,if=virtio,format=raw,readonly=on \
	-virtfs local,path=/control,mount_tag=control,security_model=none,multidevs=remap \
	-qmp unix:/control/qmp.sock,server=on,wait=off >/dev/null

for ignored in $(seq 1 200); do
	[[ -S "$control_dir/qmp.sock" ]] && break
	sleep 0.1
done
[[ -S "$control_dir/qmp.sock" ]]

docker exec "$container" /control/meld destructive-qemu-eio \
	--result "/control/$trial-result.json" --qmp-socket /control/qmp.sock \
	--proof "/control/$trial-proof.json" --ack "/control/$trial-ack.json" \
	--target-image /control/target-eio.img --blkdebug-config /control/blkdebug-eio.conf --timeout 3m \
	>"$control_dir/$trial-controller.stdout.json"

for ignored in $(seq 1 600); do
	state=$(docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null || true)
	[[ "$state" == false ]] && break
	sleep 0.1
done
docker wait "$container" >/dev/null

docker run --rm -v "$control_dir:/control" "$qemu_image" /control/meld destructive-eio-result-check \
	--result "/control/$trial-result.json" >"$control_dir/$trial-result-check.json"
docker run --rm -v "$control_dir:/control" "$qemu_image" /control/meld destructive-qemu-eio-proof-check \
	--proof "/control/$trial-proof.json" --result "/control/$trial-result.json" >"$control_dir/$trial-proof-check.json"
docker run --rm -v "$control_dir:/control" "$qemu_image" /control/meld destructive-eio-bundle-check \
	--seed /control/eio-seed.json --result "/control/$trial-result.json" --proof "/control/$trial-proof.json" \
	>"$control_dir/$trial-bundle-check.json"

printf 'write-eio verified result=%s proof=%s\n' \
	"$(sed -n 's/.*"resultSha256":"\([0-9a-f]*\)".*/\1/p' "$control_dir/$trial-result-check.json")" \
	"$(sed -n 's/.*"proofSha256":"\([0-9a-f]*\)".*/\1/p' "$control_dir/$trial-proof-check.json")"
