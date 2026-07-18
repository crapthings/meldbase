#!/bin/sh
set -eu

qualification_failure() {
	status=$?
	trap - EXIT
	echo "MELDBASE_EIO_QUALIFICATION_FAILED status=$status" >/dev/console
	poweroff -f
	sleep 10
}
trap qualification_failure EXIT

export PATH=/usr/bin:/usr/sbin:/bin:/sbin
busybox=/bin/busybox
[ -x "$busybox" ] || busybox=/usr/bin/busybox
"$busybox" --install -s /bin

mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t devtmpfs devtmpfs /dev
mdev -s

modprobe virtio_blk
mdev -s

trial=
for argument in $(cat /proc/cmdline); do
	case "$argument" in
		meld_trial=*) trial=${argument#meld_trial=} ;;
	esac
done
case "$trial" in
	*[!a-zA-Z0-9._-]*|'') echo "invalid meld_trial" >/dev/console; poweroff -f ;;
esac

mkdir -p /modloop /target /control /lib/modules
modprobe squashfs
mount -t squashfs -o ro /dev/vdb /modloop
mount -o bind /modloop/modules /lib/modules
modprobe ext4
modprobe 9pnet_virtio
modprobe 9p
mount -t ext4 -o rw /dev/vda /target
mount -t 9p -o trans=virtio,version=9p2000.L,msize=1048576 control /control

chmod 0777 /control
result="/control/${trial}-result.json"
artifact="/control/${trial}-recovered.meld"
ack="/control/${trial}-ack.json"

su nobody -s /bin/sh -c "/control/meld destructive-eio-worker \
	--database /target/source.meld --artifact '$artifact' --out '$result'" \
	>"/control/${trial}-worker.stdout.json"
echo "MELDBASE_EIO_WORKER_COMPLETE $trial" >/dev/console

for ignored in $(seq 1 1200); do
	test -s "$ack" && break
	sleep 0.1
done
test -s "$ack"

echo "MELDBASE_EIO_PROOF_COMPLETE $trial" >/dev/console
trap - EXIT
poweroff -f
