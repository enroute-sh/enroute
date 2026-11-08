#!/bin/sh
# Installs the `enroute` binary from a GitHub release.
#
#   curl -fsSL https://enroute.sh/install | sh
#
# ENROUTE_VERSION picks a release, for example `0.4.0`. The default is the
# latest. ENROUTE_INSTALL_DIR picks the directory. The default is
# `~/.local/bin`, so that the script does not need root.
#
# Everything is in functions and `main` is called on the last line, so a
# download that stops early runs nothing.

set -eu

REPO=enroute-sh/enroute

say() {
	printf 'enroute: %s\n' "$*" >&2
}

fail() {
	say "$*"
	exit 1
}

target() {
	os="$(uname -s)"
	arch="$(uname -m)"
	case "${os}/${arch}" in
	Linux/x86_64 | Linux/amd64) linux x86_64 ;;
	Linux/aarch64 | Linux/arm64) linux aarch64 ;;
	Darwin/arm64) echo aarch64-apple-darwin ;;
	*) fail "no release binary for ${os} ${arch}. Build from source or run the image: https://github.com/${REPO}#im-intrigued-how-can-i-try-it-out" ;;
	esac
}

# The Linux binaries link against glibc 2.28. A loader error after install
# says less than this does.
linux() {
	glibc="$(getconf GNU_LIBC_VERSION 2>/dev/null || true)"
	case "${glibc}" in
	"glibc "*) ;;
	*) fail "the Linux binaries need glibc, and this system does not report one (musl?). Run the image instead." ;;
	esac
	minor="${glibc#glibc 2.}"
	minor="${minor%%.*}"
	if [ "${minor}" -lt 28 ]; then
		fail "the Linux binaries need glibc 2.28 or later, and this system has ${glibc#glibc }."
	fi
	echo "$1-unknown-linux-gnu"
}

latest() {
	url="$(curl -fsSLI -o /dev/null -w '%{url_effective}' "https://github.com/${REPO}/releases/latest")" \
		|| fail "could not reach github.com."
	tag="${url##*/}"
	case "${tag}" in
	v[0-9]*) echo "${tag#v}" ;;
	*) fail "could not find the latest release." ;;
	esac
}

sha256() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1"
	elif command -v shasum >/dev/null 2>&1; then
		shasum -a 256 "$1"
	else
		fail "found no sha256sum or shasum to check the download."
	fi
}

main() {
	command -v curl >/dev/null 2>&1 || fail "curl is required."
	command -v tar >/dev/null 2>&1 || fail "tar is required."

	target="$(target)"
	version="${ENROUTE_VERSION:-$(latest)}"
	version="${version#v}"
	dir="${ENROUTE_INSTALL_DIR:-${HOME}/.local/bin}"
	name="enroute-${version}-${target}"
	base="https://github.com/${REPO}/releases/download/v${version}"

	tmp="$(mktemp -d)"
	trap 'rm -rf "${tmp}"' EXIT

	say "downloading ${name}"
	curl -fsSL -o "${tmp}/${name}.tar.gz" "${base}/${name}.tar.gz" \
		|| fail "could not download ${base}/${name}.tar.gz"
	curl -fsSL -o "${tmp}/SHA256SUMS" "${base}/SHA256SUMS" \
		|| fail "could not download ${base}/SHA256SUMS"

	expected="$(awk -v f="${name}.tar.gz" '$2 == f || $2 == "*" f { print $1 }' "${tmp}/SHA256SUMS")"
	actual="$(cd "${tmp}" && sha256 "${name}.tar.gz" | awk '{ print $1 }')"
	[ -n "${expected}" ] || fail "SHA256SUMS has no entry for ${name}.tar.gz."
	[ "${expected}" = "${actual}" ] || fail "checksum mismatch for ${name}.tar.gz."

	tar -xzf "${tmp}/${name}.tar.gz" -C "${tmp}"
	mkdir -p "${dir}"
	# Through a temporary name, so that a running `enroute` keeps its file.
	cp "${tmp}/${name}/enroute" "${dir}/.enroute.new"
	chmod 755 "${dir}/.enroute.new"
	mv -f "${dir}/.enroute.new" "${dir}/enroute"

	say "installed ${version} to ${dir}/enroute"
	case ":${PATH}:" in
	*":${dir}:"*) ;;
	*) say "${dir} is not on your PATH. Add it, for example: export PATH=\"${dir}:\$PATH\"" ;;
	esac
}

main "$@"
