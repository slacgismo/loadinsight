NODE_VERSION=13.0.1
dpkgArch="$(dpkg --print-architecture)"
case "${dpkgArch##*-}" in 
 amd64) ARCH='x64';;
 ppc64el) ARCH='ppc64le';;
 s390x) ARCH='s390x';;
 arm64) ARCH='arm64';;
 armhf) ARCH='armv7l';;
 i386) ARCH='x86';;
 *) echo "unsupported architecture"; exit 1 ;;
 esac
PLATFORM=$(uname -s | tr '[:upper:]' '[:lower:]')

buildDeps='xz-utils' 

apt-get update && apt-get install -y ca-certificates curl wget gnupg dirmngr $buildDeps --no-install-recommends
rm -rf /var/lib/apt/lists/*
# Add key to the gpg
for key in \
    94AE36675C464D64BAFA68DD7434390BDBE9B9C5 \
    FD3A5288F042B6850C66B31F09FE44734EB7990E \
    71DCFD284A79C3B38668286BC97EC7A07EDE3FC1 \
    DD8F2338BAE7501E3DD5AC78C273792F7D83545D \
    C4F0DFFF4E8C1A8236409D08E73BC641CC11F4C8 \
    B9AE9905FFD7803F25714661B63B535A4C206CA9 \
    77984A986EBC2AA786BC0F66B01FBB92821C587A \
    8FCCA13FEF1D0C2E91008E09770F7A9A5AE15600 \
    4ED778F539E3634C779C87C6D7062848A1AB005C \
    A48C2BEE680E841632CD4E44F07496B3EB3C1762 \
    B9E2F5981AA6E0CD28160D9FF13993A75599653C \
; do \
    gpg --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys "$key" || \
    gpg --keyserver hkp://ipv4.pool.sks-keyservers.net --recv-keys "$key" || \
    gpg --keyserver hkp://pgp.mit.edu:80 --recv-keys "$key" ; \
done 
curl -O "https://nodejs.org/dist/v$NODE_VERSION/node-v$NODE_VERSION-$PLATFORM-$ARCH.tar.xz"
curl -O "https://nodejs.org/dist/v$NODE_VERSION/SHASUMS256.txt"
curl -O "https://nodejs.org/dist/v$NODE_VERSION/SHASUMS256.txt.sig"
# Verify SUASUMS256.txt
gpg --verify SHASUMS256.txt.sig SHASUMS256.txt
# Verify xx.tar.xz
grep "node-v$NODE_VERSION-$PLATFORM-$ARCH.tar.xz\$" SHASUMS256.txt | sha256sum -c -
tar -xJf "node-v$NODE_VERSION-$PLATFORM-$ARCH.tar.xz" -C /usr/local --strip-components=1 --no-same-owner
rm "node-v$NODE_VERSION-$PLATFORM-$ARCH.tar.xz" SHASUMS256.txt.sig SHASUMS256.txt
apt-get purge -y --auto-remove $buildDeps
ln -s /usr/local/bin/node /usr/local/bin/nodejs