version=1.3.0

deploy: clean prepare package

clean:
	rm -rf target
	
prepare: clean
	mkdir target
	cp -r deb target/deb
	sed -i 's/\[\[version\]\]/${version}/g' target/deb/DEBIAN/control
	cp csbridge.py target/deb/usr/bin
	chmod +x target/deb/usr/bin/csbridge.py
	cp csbridge.conf target/deb/etc	

	
package: prepare
	# Fails on WSL if not mounted with metadata  
	# add metaoption to /etc/wsl.conf
	dpkg-deb -b target/deb target/bayeos-gateway-csbridge-$(version).deb


