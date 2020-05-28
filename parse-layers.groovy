
def layers = new XmlSlurper().parse(new File(System.getenv("SLD")))

def layerIds = layers.NamedLayer
        .collect {
            it.VendorOption.find {
                it.@name == 'geometryLayerId'
            }
            .text().toInteger()
        }

println(layerIds.unique(false).sort())
