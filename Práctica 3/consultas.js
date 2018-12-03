/*
* CABECERA AQUI
*/


/* AGGREGATION PIPELINE */
/*
1. (6.25 %) Listado de paı́s-número de pelı́culas rodadas en él, ordenado por número de pelı́culas
descendente y en caso de empate por nombre paı́s ascendente.
*/
function agg1(){
	return db.peliculas.aggregate(
		[
			{'$unwind': '$pais'},
			{'$group': {'_id': '$pais', 'tags': {'$sum': 1}}},
			{'$sort': {'tags': -1, '_id': 1}}
		]);
}

/*
2. (6.25 %) Listado de los 3 tipos de pelı́cula más populares entre los usuarios de los ’Emiratos
Árabes Unidos’, ordenado de mayor a menor número de usuarios que les gusta. En caso de
empate a número de usuarios, se usa el tipo de pelı́cula de manera ascendente.
*/
function agg2(){
  	return db.usuarios.aggregate(
	  	[
	  		{'$match': {'direccion.pais': 'Emiratos Árabes Unidos'}},
	  		{'$unwind': '$gustos'},
	  		{'$group': {'_id': '$gustos', 'count': {'$sum': 1}}},
	  		{'$sort': {'count': -1, '_id': 1}},
	  		{'$limit': 3}

	  	]);
}
  
/*
3. (6.25 %) Listado de paı́s-(edad mı́nima, edad-máxima, edad media) teniendo en cuenta única-
mente los usuarios mayores de edad, es decir, con más de 17 años. Los paı́ses con menos de 3
usuarios mayores de edad no deben aparecer en el resultado.
*/
function agg3(){
  	return db.usuarios.aggregate(
	  	[
	  		{'$match': {'edad': {'$gt': 17}}},
	  		{'$group': {'_id': '$direccion.pais', 'min': {'$min': '$edad'}, 'max': {'$max': '$edad'}, 'avg': {'$avg': '$edad'}, 'count': {'$sum': 1}}},
	  		{'$match': {'count': {'$gte': 3}}},
	  		{'$project': {'count': 0}}
	  	]);
}  
  
  
/*
4. (6.25 %) Listado de tı́tulo pelı́cula-número de visualizaciones de las 10 pelı́culas más vistas,
ordenado por número descencente de visualizaciones. En caso de empate, romper por tı́tulo de
pelı́cula ascendente.
*/
function agg4(){
  	return db.usuarios.aggregate(
  		[
  			{'$unwind': '$visualizaciones'},
  			{'$group': {'_id': '$visualizaciones.titulo', 'count': {'$sum': 1}}},
  			{'$sort': {'count': -1, '_id': 1}},
  			{'$limit': 10}

  		]);
}



  
/* MAPREDUCE */  
/*
1. (6 %) Listado de paı́s-número de pelı́culas rodadas en él.
*/
function mr1(){
	return db.peliculas.mapReduce(
			function() 
			{
				for (var index = 0; index < this.pais.length; index++){
					emit(this.pais[index], 1);
				}
			},
			function(key, values) 
			{
				return Array.sum(values);
			},
			{
				out: 'end'
			}
		)
}

/*
2. (6 %) Listado de rango de edad -número de usuarios. Los rangos de edad son periodos de 10 años:
[0, 10), [10, 20), [20, 30), etc. Si no hay ningun usuario con edad en un rango concreto dicho rango
no deberı́a aparecer en la salida.
*/
function mr2(){
	return db.usuarios.mapReduce(
			function() 
			{
				var begin = this.edad-(this.edad%10);
				var end = begin + 10;
				var key = '['.concat(begin).concat(',').concat(end).concat(')');
				emit(key, 1);
			},
			function(key, values)
			{
				return Array.sum(values);
			},
			{
				out: 'end'
			}
		)
}



/*
3. (7 %) Listado de paı́s-(edad mı́nima, edad-máxima, edad media) teniendo en cuenta únicamente
los usuarios con más de 17 años.
*/
function mr3(){
	return db.usuarios.mapReduce(
			function() 
			{
				emit(this.direccion.pais, {'min':this.edad, 'max':this.edad, 'avg':this.edad});
			},
			function(key, values)
			{
				var result = values.pop();
				var count = 1

                values.forEach( 
                	function(value) {
                		count++;
                		result['avg']+=value['avg'];
                		if (result['min'] > value['min']){result['min'] = value['min'];}
                		else if (result['max'] < value['max']){result['max'] = value['max'];}

                               
                    }
                );
                result['avg'] = Math.round(result['avg'] / count); // pregnutar esto
                return result;
			},
			{
				query: { edad: { $gt: 17 } },
				out: 'end'
			}
		)
}

/*
4. (6 %) Listado de año-número de visualizaciones veraniegas, donde una ((visualización veraniega))
es aquella que se ha producido en los meses de junio, julio o agosto.
*/
function mr4(){
	return db.usuarios.mapReduce(
			function() 
			{
				var months = ['06', '07', '08'];
				for (var index = 0; index < this.visualizaciones.length; index++){
					var date = this.visualizaciones[index].fecha.split('-');
					if (months.includes(date[1])){
						emit(date[0], 1);
					}
				}
			},
			function(key, values)
			{
				
                return Array.sum(values);
			},
			{
				out: 'end'
			}
		)
}