#!/usr/bin/env bash
set -euo pipefail

usage() {
	printf 'usage: %s CONTROL_DIR MELD_LINUX_AMD64 VMLINUZ_VIRT INITRAMFS_VIRT MODLOOP_VIRT [SOURCE_REVISION]\n' "$0" >&2
	exit 2
}

[[ $# -eq 5 || $# -eq 6 ]] || usage

control_dir=$1
meld_binary=$2
kernel=$3
base_initramfs=$4
modloop=$5
source_revision=${6:-}
qemu_cut_mode=${MELDBASE_QEMU_CUT_MODE:-system-reset}
session_plan=${MELDBASE_QUALIFICATION_PLAN:-}
session_uid=${MELDBASE_QUALIFICATION_UID:-65534}
dry_run=${MELDBASE_QEMU_DRY_RUN:-0}
trial_limit=${MELDBASE_QEMU_TRIAL_LIMIT:-15}
script_dir=$(cd "$(dirname "$0")" && pwd -P)
qemu_image='ghcr.io/cross-rs/x86_64-unknown-linux-gnu@sha256:65edeb793308323d185cbfb903778debfff6258c74934ef6a4993ffcdb9763cb'
initramfs_image='alpine@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce'
container="meld-qemu-matrix-$$"
session_matrix_complete=0

[[ -d "$control_dir" && -f "$meld_binary" && -f "$kernel" && -f "$base_initramfs" && -f "$modloop" ]] || usage
control_dir=$(cd "$control_dir" && pwd -P)
for input in "$meld_binary" "$kernel" "$base_initramfs" "$modloop"; do
	[[ -r "$input" ]] || usage
done
if [[ -n "$source_revision" && ! "$source_revision" =~ ^([0-9a-fA-F]{40}|[0-9a-fA-F]{64})$ ]]; then
	printf 'SOURCE_REVISION must be 40 or 64 hexadecimal characters\n' >&2
	exit 2
fi
if [[ "$qemu_cut_mode" != system-reset && "$qemu_cut_mode" != host-sigkill ]]; then
	printf 'MELDBASE_QEMU_CUT_MODE must be system-reset or host-sigkill\n' >&2
	exit 2
fi
if [[ ! "$session_uid" =~ ^[1-9][0-9]*$ || "$session_uid" -gt 2147483647 ]]; then
	printf 'MELDBASE_QUALIFICATION_UID must be a positive non-root integer\n' >&2
	exit 2
fi
if [[ ! "$trial_limit" =~ ^([1-9]|1[0-5])$ ]]; then
	printf 'MELDBASE_QEMU_TRIAL_LIMIT must be between 1 and 15\n' >&2
	exit 2
fi

session_plan_container=
if [[ -n "$session_plan" ]]; then
	[[ "$trial_limit" -eq 15 ]] || { printf 'session mode requires the complete 15-trial power matrix\n' >&2; exit 2; }
	[[ -n "$source_revision" ]] || { printf 'session mode requires SOURCE_REVISION\n' >&2; exit 2; }
	[[ -f "$session_plan" && ! -L "$session_plan" ]] || { printf 'MELDBASE_QUALIFICATION_PLAN must be a regular plan file\n' >&2; exit 2; }
	session_plan_directory=$(cd "$(dirname "$session_plan")" && pwd -P)
	session_plan="$session_plan_directory/$(basename "$session_plan")"
	case "$session_plan" in
		"$control_dir"/*) session_plan_container="/control/${session_plan#"$control_dir"/}" ;;
		*) printf 'MELDBASE_QUALIFICATION_PLAN must be inside CONTROL_DIR\n' >&2; exit 2 ;;
	esac
fi

if [[ "$dry_run" == 1 ]]; then
	boundaries=(after-page-write before-data-sync after-data-sync after-meta-write after-meta-sync)
	ordinal=0
	for boundary in "${boundaries[@]}"; do
		for repetition in 1 2 3; do
			ordinal=$((ordinal + 1))
			(( ordinal <= trial_limit )) || break 2
			if [[ -n "$session_plan_container" ]]; then
				boundary_ordinal=$(((ordinal - 1) / 3 + 1))
				printf 'trial=power-%02d-%02d boundary=%s mode=%s session=true\n' "$boundary_ordinal" "$repetition" "$boundary" "$qemu_cut_mode"
			else
				printf 'trial=matrix-%02d-%s-%d boundary=%s mode=%s session=false\n' "$ordinal" "$boundary" "$repetition" "$boundary" "$qemu_cut_mode"
			fi
		done
	done
	exit 0
fi

cleanup() {
	docker rm -f "$container" >/dev/null 2>&1 || true
	rm -f "$control_dir/qmp.sock"
	if [[ -z "$session_plan_container" || "$session_matrix_complete" -eq 1 ]]; then
		rm -f "$control_dir/target.img"
	fi
}
trap cleanup EXIT INT TERM

cp "$meld_binary" "$control_dir/meld"
cp "$kernel" "$control_dir/vmlinuz-virt"
cp "$base_initramfs" "$control_dir/initramfs-virt"
cp "$modloop" "$control_dir/modloop-virt"
cp "$script_dir/destructive-qemu-guest-init.sh" "$control_dir/guest-init.sh"
chmod 0755 "$control_dir/meld" "$control_dir/guest-init.sh"

docker run --rm -v "$control_dir:/control" "$initramfs_image" sh -lc '
	set -eu
	rm -rf /tmp/initrd
	mkdir /tmp/initrd
	chmod 0755 /tmp/initrd
	cd /tmp/initrd
	zcat /control/initramfs-virt | cpio -idmu >/dev/null 2>&1
	cp /control/guest-init.sh init
	chmod 0755 . init
	find . -print | cpio -o -H newc 2>/dev/null | gzip -9 > /control/initramfs-meld
'

find "$control_dir" -maxdepth 1 -name 'matrix-*' -delete
verification="$control_dir/matrix-verification.jsonl"
: >"$verification"
receipt_args=()
receipt_paths=()
boundaries=(after-page-write before-data-sync after-data-sync after-meta-write after-meta-sync)
ordinal=0

session_status() {
	docker run --rm --user "$session_uid:$session_uid" -v "$control_dir:/control" "$qemu_image" \
		/control/meld qualification-session-status --plan "$session_plan_container"
}

for boundary in "${boundaries[@]}"; do
	for repetition in 1 2 3; do
		ordinal=$((ordinal + 1))
		(( ordinal <= trial_limit )) || break 2
		if [[ -n "$session_plan_container" ]]; then
			boundary_ordinal=$(((ordinal - 1) / 3 + 1))
			printf -v trial 'power-%02d-%02d' "$boundary_ordinal" "$repetition"
			expected_completed=$((ordinal + 4))
			status=$(session_status)
			completed=$(printf '%s\n' "$status" | sed -n 's/.*"completed":\([0-9][0-9]*\).*/\1/p')
			[[ -n "$completed" ]] || { printf 'cannot parse qualification session status\n' >&2; exit 1; }
			if (( completed >= expected_completed )); then
				[[ -s "$control_dir/$trial-recovery.json" ]] || { printf 'recorded session receipt is missing: %s\n' "$trial" >&2; exit 1; }
				receipt_args+=(--receipt "/control/$trial-recovery.json")
				receipt_paths+=("$control_dir/$trial-recovery.json")
				printf '%s already recorded; skipping\n' "$trial"
				continue
			fi
			if (( completed != expected_completed - 1 )) ||
				! grep -Fq '"kind":"power"' <<<"$status" ||
				! grep -Fq '"powerTrialId":"'"$trial"'"' <<<"$status" ||
				! grep -Fq '"publicationBoundary":"'"$boundary"'"' <<<"$status"; then
				printf 'qualification session is not ready for %s at %s: %s\n' "$trial" "$boundary" "$status" >&2
				exit 1
			fi
		else
			printf -v trial 'matrix-%02d-%s-%d' "$ordinal" "$boundary" "$repetition"
		fi
		if [[ -n "$session_plan_container" && -s "$control_dir/$trial-recovery.json" ]]; then
			docker run --rm -v "$control_dir:/control" "$qemu_image" /control/meld destructive-power-receipt-check \
				--receipt "/control/$trial-recovery.json" >/dev/null
			docker run --rm --user "$session_uid:$session_uid" -v "$control_dir:/control" "$qemu_image" \
				/control/meld qualification-session-record --plan "$session_plan_container" \
				--kind power --receipt "/control/$trial-recovery.json" >/dev/null
			receipt_args+=(--receipt "/control/$trial-recovery.json")
			receipt_paths+=("$control_dir/$trial-recovery.json")
			printf '%s recovered and recorded from its existing receipt\n' "$trial"
			continue
		fi

		resume_trial=0
		controller_ready=0
		if [[ -n "$session_plan_container" && -e "$control_dir/$trial-marker.json" ]]; then
			[[ -s "$control_dir/$trial-marker.json" && -s "$control_dir/target.img" ]] || {
				printf 'incomplete %s has no recoverable marker/target pair; preserve it for inspection\n' "$trial" >&2
				exit 1
			}
			resume_trial=1
			if [[ -s "$control_dir/$trial-controller.json" && -s "$control_dir/$trial-qmp-proof.json" ]]; then
				controller_ready=1
			elif [[ -e "$control_dir/$trial-controller.json" || -e "$control_dir/$trial-qmp-proof.json" ]]; then
				printf 'incomplete %s has only part of its controller evidence\n' "$trial" >&2
				exit 1
			fi
		fi
		docker rm -f "$container" >/dev/null 2>&1 || true
		rm -f "$control_dir/qmp.sock"
		if [[ "$resume_trial" -eq 0 ]]; then
			if [[ -n "$session_plan_container" &&
				( -e "$control_dir/$trial-controller.json" || -e "$control_dir/$trial-qmp-proof.json" || -e "$control_dir/$trial-token" ) ]]; then
				printf 'incomplete %s artifacts exist without a marker; preserve them for inspection\n' "$trial" >&2
				exit 1
			fi
			rm -f "$control_dir/target.img"
			docker run --rm -v "$control_dir:/control" "$qemu_image" sh -lc '
				truncate -s 128M /control/target.img
				mkfs.ext4 -q -F /control/target.img
			'
		fi

		append="console=ttyS0 panic=-1 meld_boundary=$boundary meld_trial=$trial"
		if [[ -n "$source_revision" ]]; then
			append="$append meld_revision=$source_revision"
		fi
		qemu_args=(
			-machine accel=tcg -cpu max -m 512 -display none -serial "file:/control/$trial-qemu.log"
			-kernel /control/vmlinuz-virt -initrd /control/initramfs-meld -append "$append"
			-drive file=/control/target.img,if=virtio,format=raw,cache=none,aio=threads
			-drive file=/control/modloop-virt,if=virtio,format=raw,readonly=on
			-virtfs local,path=/control,mount_tag=control,security_model=none,multidevs=remap
			-qmp unix:/control/qmp.sock,server=on,wait=off
		)
		if [[ "$qemu_cut_mode" == system-reset || "$controller_ready" -eq 1 ]]; then
			docker run -d --name "$container" --privileged -v "$control_dir:/control" "$qemu_image" \
				/usr/local/bin/qemu-system-x86_64 "${qemu_args[@]}" >/dev/null
			for _ in $(seq 1 100); do
				docker exec "$container" test -S /control/qmp.sock && break
				sleep 0.1
			done
			if [[ "$controller_ready" -eq 0 ]]; then
				docker exec "$container" /control/meld destructive-qemu-reset \
					--marker "/control/$trial-marker.json" --qmp-socket /control/qmp.sock \
					--proof "/control/$trial-qmp-proof.json" --out "/control/$trial-controller.json" --timeout 2m \
					>"$control_dir/$trial-controller.stdout.json"
				# security_model=none maps guest files to the QEMU uid. Restore the
				# qualification uid without making the private proof world-readable.
				docker exec "$container" chown "$session_uid:$session_uid" "/control/$trial-controller.json" "/control/$trial-qmp-proof.json"
			fi
			for _ in $(seq 1 600); do
				[[ -s "$control_dir/$trial-recovery.json" ]] && break
				sleep 0.2
			done
		else
			docker run --rm --name "$container" --privileged -v "$control_dir:/control" "$qemu_image" \
				/control/meld destructive-qemu-process-kill \
				--marker "/control/$trial-marker.json" --qmp-socket /control/qmp.sock \
				--proof "/control/$trial-qmp-proof.json" --out "/control/$trial-controller.json" \
				--qemu-log "/control/$trial-qemu-process.log" --recovery-receipt "/control/$trial-recovery.json" \
				--target-image /control/target.img \
				--artifact-uid 65534 --artifact-gid 65534 --timeout 3m -- \
				/usr/local/bin/qemu-system-x86_64 "${qemu_args[@]}" \
				>"$control_dir/$trial-controller.stdout.json"
		fi
		[[ -s "$control_dir/$trial-recovery.json" ]]
		docker run --rm -v "$control_dir:/control" "$qemu_image" /control/meld destructive-power-receipt-check \
			--receipt "/control/$trial-recovery.json" >/dev/null
		if [[ -n "$session_plan_container" ]]; then
			docker run --rm --user "$session_uid:$session_uid" -v "$control_dir:/control" "$qemu_image" \
				/control/meld qualification-session-record --plan "$session_plan_container" \
				--kind power --receipt "/control/$trial-recovery.json" >/dev/null
		fi
		receipt_args+=(--receipt "/control/$trial-recovery.json")
		receipt_paths+=("$control_dir/$trial-recovery.json")
		outcome=$(sed -n 's/.*"outcome": "\([^"]*\)".*/\1/p' "$control_dir/$trial-recovery.json")
		sequence=$(sed -n 's/.*"recoveredCommitSequence": \([0-9]*\).*/\1/p' "$control_dir/$trial-recovery.json")
		printf '%s mode=%s outcome=%s sequence=%s\n' "$trial" "$qemu_cut_mode" "$outcome" "$sequence"
	done
done

session_matrix_complete=1

for receipt in "${receipt_paths[@]}"; do
	receipt_name=$(basename "$receipt")
	docker run --rm -v "$control_dir:/control" "$qemu_image" /control/meld destructive-power-receipt-check \
		--receipt "/control/$receipt_name" >>"$verification"
done
[[ $(grep -c '"passed": true' "$verification") -eq "$trial_limit" ]]
if [[ "$trial_limit" -eq 15 ]]; then
	docker run --rm -v "$control_dir:/control" "$qemu_image" /control/meld destructive-power-matrix-check \
		"${receipt_args[@]}" >"$control_dir/matrix-summary.json"
else
	rm -f "$control_dir/matrix-summary.json"
fi
sha256=$(shasum -a 256 "$verification" | awk '{print $1}')
printf 'verified=%s verificationSha256=%s releaseMatrix=%s\n' "$trial_limit" "$sha256" "$([[ "$trial_limit" -eq 15 ]] && printf true || printf false)"
