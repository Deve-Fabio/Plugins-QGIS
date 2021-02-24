# -*- coding: utf-8 -*-
"""
/***************************************************************************
 GeopDetran
                                 A QGIS plugin
 Plugin para realizar consultas nos arquivos geográficos
 Generated by Plugin Builder: http://g-sherman.github.io/Qgis-Plugin-Builder/
                             -------------------
        begin                : 2021-01-28
        copyright            : (C) 2021 by Francisco Fábio - Detran - DF
        email                : nugeo@detran.df.gov.br
        git sha              : $Format:%H$
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
 This script initializes the plugin, making it known to QGIS.
"""


# noinspection PyPep8Naming
def classFactory(iface):  # pylint: disable=invalid-name
    """Load GeopDetran class from file GeopDetran.

    :param iface: A QGIS interface instance.
    :type iface: QgsInterface
    """
    #
    from .Geop_Detran import GeopDetran
    return GeopDetran(iface)
