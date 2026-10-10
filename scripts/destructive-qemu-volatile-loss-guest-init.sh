#!/bin/sh
set -eu

# Reports an unexpected guest exit and powers off the test machine.
qualification_failure() {
	status=$?
	trap - EXIT
	echo "MELDBASE_VOLATILE_LOSS_NEGATIVE_CONTROL_FAILED status=$status" >/dev/console
	poweroff -f
	sleep 10
}
trap qualification_failure EXIT

# Selects the guest directories used to find system commands.
export PATH=/usr/bin:/usr/sbin:/bin:/sbin
# Locates BusyBox so its commands can be installed in the guest.
busybox=/bin/busybox
[ -x "$busybox" ] || busybox=/usr/bin/busybox
"$busybox" --install -s /bin
mount -t proc proc /proc
mount -t sysfs sysfs /sys
mount -t devtmpfs devtmpfs /dev
mdev -s
modprobe virtio_blk
mdev -s

# Holds the trial name read from the guest boot arguments.
trial=
for argument in $(cat /proc/cmdline); do
	case "$argument" in meld_trial=*) trial=${argument#meld_trial=} ;; esac
done
case "$trial" in *[!a-zA-Z0-9._-]*|'') echo "invalid meld_trial" >/dev/console; poweroff -f ;; esac

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

# Names the update marker used to distinguish the initial boot from recovery.
marker="/control/${trial}-marker.json"
# Names the receipt telling the host that recovery has started.
ready="/control/${trial}-recovery-ready.json"
# Names the host controller record required for recovery.
event="/control/${trial}-controller.json"
# Names the host evidence of the simulated volatile data loss.
proof="/control/${trial}-proof.json"
# Names the recovery result shared with the host.
result="/control/${trial}-result.json"
# Names the recovered database retained as test evidence.
artifact="/control/${trial}-recovered.meld"

if [ ! -f "$marker" ]; then
	su nobody -s /bin/sh -c "/control/meld destructive-volatile-loss-update \
		--database /target/power.meld --marker '$marker'" \
		>"/control/${trial}-update.stdout.json"
	echo "volatile-loss update unexpectedly returned" >/dev/console
	exit 1
fi

su nobody -s /bin/sh -c "/control/meld destructive-volatile-loss-recovery-ready \
	--marker '$marker' --out '$ready'" >"/control/${trial}-recovery-ready.stdout.json"
for ignored in $(seq 1 1800); do
	su nobody -s /bin/sh -c "test -s '$event' && test -r '$event' && test -s '$proof' && test -r '$proof'" && break
	sleep 0.1
done
su nobody -s /bin/sh -c "test -s '$event' && test -r '$event' && test -s '$proof' && test -r '$proof'"
su nobody -s /bin/sh -c "/control/meld destructive-volatile-loss-recover \
	--database /target/power.meld --marker '$marker' --controller '$event' --proof '$proof' \
	--artifact '$artifact' --out '$result'" >"/control/${trial}-recovery.stdout.json"
echo "MELDBASE_VOLATILE_LOSS_UNSAFE_STORAGE_DETECTED $trial" >/dev/console
trap - EXIT
poweroff -f
