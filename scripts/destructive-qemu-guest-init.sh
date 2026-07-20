#!/bin/sh
set -eu

qualification_failure() {
	status=$?
	trap - EXIT
	echo "MELDBASE_POWER_QUALIFICATION_FAILED status=$status" >/dev/console
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

boundary=
trial=
revision=
for argument in $(cat /proc/cmdline); do
	case "$argument" in
		meld_boundary=*) boundary=${argument#meld_boundary=} ;;
		meld_trial=*) trial=${argument#meld_trial=} ;;
		meld_revision=*) revision=${argument#meld_revision=} ;;
	esac
done
case "$boundary" in
	after-page-write|before-data-sync|after-data-sync|after-meta-write|after-meta-sync) ;;
	*) echo "invalid meld_boundary" >/dev/console; poweroff -f ;;
esac
case "$trial" in
	*[!a-zA-Z0-9._-]*|'') echo "invalid meld_trial" >/dev/console; poweroff -f ;;
esac
if [ -n "$revision" ]; then
	case "$revision" in
		*[!0-9a-fA-F]*) echo "invalid meld_revision" >/dev/console; poweroff -f ;;
	esac
	[ "${#revision}" -eq 40 ] || [ "${#revision}" -eq 64 ] || { echo "invalid meld_revision" >/dev/console; poweroff -f; }
fi

mkdir -p /modloop /target /control /lib/modules
modprobe squashfs
mount -t squashfs -o ro /dev/vdb /modloop
mount -o bind /modloop/modules /lib/modules
modprobe ext4
modprobe 9pnet_virtio
modprobe 9p
mount -t ext4 -o rw /dev/vda /target
mount -t 9p -o trans=virtio,version=9p2000.L,msize=1048576 control /control

chown 65534:65534 /target
chmod 0700 /target
chmod 0777 /control

marker="/control/${trial}-marker.json"
event="/control/${trial}-controller.json"
proof="/control/${trial}-qmp-proof.json"
receipt="/control/${trial}-recovery.json"
token_file="/control/${trial}-token"

if [ ! -f "$marker" ]; then
	su nobody -s /bin/sh -c "/control/meld destructive-volume-check --dir /target --control-dir /control" >"/control/${trial}-volume.json"
	token=$(sed -n 's/.*"destructiveToken": "\([^"]*\)".*/\1/p' "/control/${trial}-volume.json")
	[ -n "$token" ]
	printf '%s\n' "$token" >"$token_file"
	sync
	revision_args=
	if [ -n "$revision" ]; then
		revision_args="--source-revision '$revision' --require-clean-source"
	fi
	su nobody -s /bin/sh -c "/control/meld destructive-power-prepare \
		--dir /target --control-dir /control --marker '$marker' --trial-id '$trial' \
		--boundary '$boundary' --destructive-token '$token' $revision_args"
	echo "power prepare unexpectedly returned" >/dev/console
	poweroff -f
fi

for ignored in $(seq 1 600); do
	su nobody -s /bin/sh -c "test -s '$event' && test -r '$event' && test -s '$proof' && test -r '$proof'" && break
	sleep 0.1
done
su nobody -s /bin/sh -c "test -s '$event' && test -r '$event' && test -s '$proof' && test -r '$proof'"
token=$(sed -n '1p' "$token_file")
su nobody -s /bin/sh -c "/control/meld destructive-power-recover \
	--dir /target --control-dir /control --marker '$marker' \
	--controller-event '$event' --controller-proof '$proof' --out '$receipt' \
	--destructive-token '$token'" >"/control/${trial}-recovery.stdout.json"
sync
echo "MELDBASE_POWER_RECOVERY_COMPLETE $trial" >/dev/console
trap - EXIT
poweroff -f
