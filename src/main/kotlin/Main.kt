fun main(args: Array<String>) {
    confdatabase(args)
}

private fun confdatabase(args: Array<String>) {
    val c = ConnectionBuilder()
    println("conectando.....")

    if (c.connection.isValid(10)) {
        println("Conexion Valida")
        c.connection.use {
            val conctf = CtfDao(c.connection)
            val congrupo = GrupoDao(c.connection)
            conctf.prepareTable()
            congrupo.prepareTable()
            congrupo.insertGrupo(Grupo(grupoid = 1, grupodesc = "1DAM-G1", mejorposCTFid = 0))
            congrupo.insertGrupo(Grupo(grupoid = 2, grupodesc = "1DAM-G2", mejorposCTFid = 0))
            congrupo.insertGrupo(Grupo(grupoid = 3, grupodesc = "1DAM-G3", mejorposCTFid = 0))
            congrupo.insertGrupo(Grupo(grupoid = 4, grupodesc = "1DAM-G1", mejorposCTFid = 0))
            congrupo.insertGrupo(Grupo(grupoid = 5, grupodesc = "1DAM-G2", mejorposCTFid = 0))
            congrupo.insertGrupo(Grupo(grupoid = 6, grupodesc = "1DAM-G3", mejorposCTFid = 0))

            if (args.size == 4 && args.contains("-a")) {
                val primerdato = args[1].toInt()
                val segundato = args[2].toInt()
                val tercerdato = args[3].toInt()
                conctf.insertCTFs(Ctf(CTFid = primerdato, grupoId = segundato, puntuacion = tercerdato))
                println("Procesado: Añadida participación del grupo $primerdato en el CTF $segundato con una puntuación de $tercerdato puntos.")
                val lista = conctf.selectAllCTFS()
                val mejoresCtfByGroupId = calculaMejoresResultados(lista)
                mejoresCtfByGroupId.values.forEach{ mejoresCtfByGroupId ->
                    var grupo = congrupo.selectById(mejoresCtfByGroupId.second.grupoId)
                    grupo?.let { elGrupo ->
                        elGrupo.mejorposCTFid = mejoresCtfByGroupId.second.CTFid
                        congrupo.updateGrupo(elGrupo.grupoid,elGrupo.mejorposCTFid)
                    }
                }
            } else if (args.size == 3 && args.contains("-d")) {
                val primerdato = args[1].toInt()
                val segundato = args[2].toInt()
                val res = conctf.deleteCTFSById(primerdato, segundato)
                if (res == true) {
                    println("Procesado: Eliminada participación del grupo $primerdato en el CTF $segundato.")
                } else {
                    println("Ha ocurrido un error")
                }
            } else if (args.size == 2 && args.contains("-l")) {
                val primerdato = args[1].toInt()
                val mosgrupo = congrupo.selectById(primerdato)
                val mostodo = congrupo.selectAllGrupos()
                if (mosgrupo != null) {
                    print(mosgrupo)
                } else {
                    print(mostodo)
                }
            } else if (args.size == 1 && args.contains("-l")) {
                val todo = congrupo.selectAllGrupos()
                print(todo)
            } else {
                println("ERROR: El número de parametros no es adecuado.")
            }
        }
    } else {
        println("Error de Conexion")
    }
}

private fun calculaMejoresResultados(participaciones: List<Ctf>): MutableMap<Int, Pair<Int, Ctf>> {
    val participacionesByCTFId = participaciones.groupBy { it.CTFid }
    var participacionesByGrupoId = participaciones.groupBy { it.grupoId }
    val mejoresCtfByGroupId = mutableMapOf<Int, Pair<Int, Ctf>>()
    participacionesByCTFId.values.forEach { ctfs ->
        val ctfsOrderByPuntuacion = ctfs.sortedBy { it.puntuacion }.reversed()
        participacionesByGrupoId.keys.forEach { grupoId ->
            val posicionNueva = ctfsOrderByPuntuacion.indexOfFirst { it.grupoId == grupoId }
            if (posicionNueva >= 0) {
                val posicionMejor = mejoresCtfByGroupId.getOrDefault(grupoId, null)
                if (posicionMejor != null) {
                    if (posicionNueva < posicionMejor.first)
                        mejoresCtfByGroupId.set(grupoId, Pair(posicionNueva, ctfsOrderByPuntuacion.get(posicionNueva)))
                } else
                    mejoresCtfByGroupId.set(grupoId, Pair(posicionNueva, ctfsOrderByPuntuacion.get(posicionNueva)))

            }
        }
    }
    return mejoresCtfByGroupId
}