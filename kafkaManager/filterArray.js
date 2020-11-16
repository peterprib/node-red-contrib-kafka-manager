function filterArray(data=[],filter,key){
	if(filter==null) throw Error("no filter");
	if(Array.isArray(filter)){
		const filters=filter.map(topic=> new RegExp(topic, 'g'));
		if(key) return data.filter(cell=>filters.find(filter=>cell[key].match(filter)));
		return data.filter(cell=>filters.find(filter=>filter.test(cell)));
	}
	const regex = new RegExp(filter, 'g');
	if(key) return data.filter(cell=>cell[key].match(regex))
	return data.filter(cell=>cell.match(regex));
}
module.exports=filterArray