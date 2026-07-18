#!/usr/bin/env bash
set -euo pipefail

usage() {
	printf 'usage: %s ARTIFACTS_ROOT TARGET_DIR MELD_BINARY SOURCE_REVISION PLATFORM_CLASS CONTROLLER_METHOD OPERATOR_EVIDENCE [CONTROLLER_PUBLIC_KEY CONTROLLER_TARGET_IDENTITY_SHA256]\n' "$0" >&2
	exit 2
}

[[ $# -eq 7 || $# -eq 9 ]] || usage

artifacts_root=$1
target_dir=$2
meld_binary=$3
source_revision=$4
platform_class=$5
controller_method=$6
operator_evidence=$7
controller_public_key=${8:-}
controller_target_identity_sha256=${9:-}

[[ "$source_revision" =~ ^([0-9a-fA-F]{40}|[0-9a-fA-F]{64})$ ]] || usage
[[ "$platform_class" =~ ^[a-zA-Z0-9._-]+$ ]] || usage
case "$controller_method" in
	qemu-system-reset|qemu-host-sigkill) [[ -z "$controller_public_key" && -z "$controller_target_identity_sha256" ]] || usage ;;
	hypervisor-hard-reset|ipmi-chassis-power-cycle|pdu-power-cycle|redfish-computer-system-power-cycle)
		[[ -n "$controller_public_key" && "$controller_target_identity_sha256" =~ ^[0-9a-f]{64}$ ]] || usage
		;;
	*) usage ;;
esac
[[ -d "$artifacts_root" && ! -L "$artifacts_root" && -d "$target_dir" && ! -L "$target_dir" ]] || usage
[[ -f "$meld_binary" && ! -L "$meld_binary" && -x "$meld_binary" ]] || usage
[[ -f "$operator_evidence" && ! -L "$operator_evidence" && -s "$operator_evidence" ]] || usage
if [[ -n "$controller_public_key" ]]; then
	[[ -f "$controller_public_key" && ! -L "$controller_public_key" && -s "$controller_public_key" ]] || usage
fi

artifacts_root=$(cd "$artifacts_root" && pwd -P)
target_dir=$(cd "$target_dir" && pwd -P)
meld_binary_directory=$(cd "$(dirname "$meld_binary")" && pwd -P)
meld_binary="$meld_binary_directory/$(basename "$meld_binary")"
operator_evidence_directory=$(cd "$(dirname "$operator_evidence")" && pwd -P)
operator_evidence="$operator_evidence_directory/$(basename "$operator_evidence")"
if [[ -n "$controller_public_key" ]]; then
	controller_public_key_directory=$(cd "$(dirname "$controller_public_key")" && pwd -P)
	controller_public_key="$controller_public_key_directory/$(basename "$controller_public_key")"
fi

case "$operator_evidence" in
	"$artifacts_root"/*) ;;
	*) printf 'OPERATOR_EVIDENCE must be retained inside ARTIFACTS_ROOT\n' >&2; exit 2 ;;
esac
if [[ -n "$controller_public_key" ]]; then
	case "$controller_public_key" in
		"$artifacts_root"/*) ;;
		*) printf 'CONTROLLER_PUBLIC_KEY must be retained inside ARTIFACTS_ROOT\n' >&2; exit 2 ;;
	esac
fi
case "$artifacts_root" in
	"$target_dir"|"$target_dir"/*) printf 'ARTIFACTS_ROOT must be outside TARGET_DIR\n' >&2; exit 2 ;;
esac
case "$meld_binary" in
	"$target_dir"/*) printf 'MELD_BINARY must be outside TARGET_DIR\n' >&2; exit 2 ;;
esac

if [[ ${MELDBASE_QUALIFICATION_DRY_RUN:-0} == 1 ]]; then
	printf 'stage=environment controller=%s attested=%s\n' "$controller_method" "$([[ -n "$controller_public_key" ]] && printf true || printf false)"
	printf 'stage=session-init platform=%s revision=%s\n' "$platform_class" "$source_revision"
	printf 'stage=durability\nstage=soak profile=release\nstage=process trials=20\nstage=capacity boundaries=5 repetitions=3\nstage=corruption pageSamples=128\n'
	exit 0
fi

[[ $(uname -s) == Linux && ${BASH_VERSINFO[0]} -ge 4 ]] || {
	printf 'qualification foundation requires Linux and Bash 4 or newer\n' >&2
	exit 2
}
[[ $(id -u) -ne 0 ]] || { printf 'qualification foundation must run as a non-root operator\n' >&2; exit 2; }

umask 077
mkdir -p "$artifacts_root/infrastructure" "$artifacts_root/receipts" "$artifacts_root/artifacts/process"

environment="$artifacts_root/infrastructure/qualification-environment.json"
plan="$artifacts_root/.qualification-session/plan.json"
durability="$artifacts_root/receipts/durability-check.json"
soak="$artifacts_root/receipts/storage-soak.json"
process="$artifacts_root/receipts/process-kill.json"
capacity="$artifacts_root/enospc-receipt.json"
corruption="$artifacts_root/receipts/corruption.json"

if [[ ! -e "$plan" ]]; then
	if [[ ! -e "$environment" ]]; then
		environment_args=(qualification-environment-capture \
			--dir "$target_dir" --control-dir "$artifacts_root" \
			--controller-method "$controller_method" --operator-evidence "$operator_evidence" \
			--source-revision "$source_revision" --out "$environment")
		if [[ -n "$controller_public_key" ]]; then
			environment_args+=(--controller-public-key "$controller_public_key" \
				--controller-target-identity-sha256 "$controller_target_identity_sha256")
		fi
		"$meld_binary" "${environment_args[@]}" >/dev/null
	fi
	"$meld_binary" qualification-session-init \
		--artifacts-root "$artifacts_root" --environment-record "$environment" \
		--source-revision "$source_revision" --platform-class "$platform_class" >/dev/null
fi

session_status() {
	"$meld_binary" qualification-session-status --plan "$plan"
}

status_field_number() {
	local name=$1
	sed -n 's/.*"'"$name"'":\([0-9][0-9]*\).*/\1/p'
}

status_next_kind() {
	sed -n 's/.*"next":{[^}]*"kind":"\([a-z]*\)".*/\1/p'
}

record_existing() {
	local kind=$1
	local receipt=$2
	if [[ -s "$receipt" ]]; then
		"$meld_binary" qualification-session-record --plan "$plan" --kind "$kind" --receipt "$receipt" >/dev/null
		printf 'recorded existing %s receipt: %s\n' "$kind" "$receipt"
		return 0
	fi
	return 1
}

while :; do
	status=$(session_status)
	completed=$(printf '%s\n' "$status" | status_field_number completed)
	[[ -n "$completed" ]] || { printf 'cannot parse qualification session status: %s\n' "$status" >&2; exit 1; }
	if (( completed >= 5 )); then
		break
	fi
	kind=$(printf '%s\n' "$status" | status_next_kind)
	case "$kind" in
		durability)
			if ! record_existing durability "$durability"; then
				"$meld_binary" durability-check --dir "$target_dir" --out "$durability" \
					--source-revision "$source_revision" --require-clean-source >/dev/null
				record_existing durability "$durability"
			fi
			;;
		soak)
			if ! record_existing soak "$soak"; then
				"$meld_binary" storage-soak --dir "$target_dir" --out "$soak" --profile release \
					--source-revision "$source_revision" --require-clean-source
				record_existing soak "$soak"
			fi
			;;
		process)
			if ! record_existing process "$process"; then
				"$meld_binary" destructive-process-check --dir "$target_dir" --out "$process" \
					--artifacts-dir "$artifacts_root/artifacts/process" --source-revision "$source_revision" \
					--require-clean-source >/dev/null
				record_existing process "$process"
			fi
			;;
		capacity)
			if ! record_existing capacity "$capacity"; then
				preflight=$("$meld_binary" destructive-volume-check --dir "$target_dir" --control-dir "$artifacts_root")
				token=$(printf '%s\n' "$preflight" | sed -n 's/.*"destructiveToken": "\([^"]*\)".*/\1/p')
				[[ -n "$token" ]] || { printf 'cannot parse destructive volume token\n' >&2; exit 1; }
				"$meld_binary" destructive-enospc-check --dir "$target_dir" --control-dir "$artifacts_root" \
					--out "$capacity" --destructive-token "$token" --source-revision "$source_revision" \
					--require-clean-source >/dev/null
				record_existing capacity "$capacity"
			fi
			;;
		corruption)
			if ! record_existing corruption "$corruption"; then
				mapfile -d '' candidates < <(find "$artifacts_root" -type f \
					-path '*/.meldbase-enospc-evidence-*/*/crash-image.meld' -print0 | sort -z)
				[[ ${#candidates[@]} -gt 0 ]] || { printf 'capacity crash image for corruption campaign is missing\n' >&2; exit 1; }
				"$meld_binary" destructive-corruption-check --database "${candidates[0]}" --page-samples 128 \
					--source-revision "$source_revision" --require-clean-source --out "$corruption" >/dev/null
				record_existing corruption "$corruption"
			fi
			;;
		*) printf 'unexpected qualification foundation step: %s\n' "$status" >&2; exit 1 ;;
	esac
done

status=$(session_status)
completed=$(printf '%s\n' "$status" | status_field_number completed)
if [[ -z "$completed" ]] || (( completed < 5 )) ||
	{ ! grep -Fq '"kind":"power"' <<<"$status" && ! grep -Fq '"readyToSeal":true' <<<"$status"; }; then
	printf 'qualification foundation did not stop at the first power step: %s\n' "$status" >&2
	exit 1
fi
printf '%s\n' "$status"
