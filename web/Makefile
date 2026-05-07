build:
	go build
deploy:
	scp polksearch polk.r.ss:
	ssh polk.r.ss "sudo mv polksearch /etc/polk-county-iowa-property-search/polk-county-iowa-property-search"
	ssh polk.r.ss "sudo systemctl restart polk-county-iowa-property-search"
