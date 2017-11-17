
import arcpy
import os
import glob
import numpy as np
import pandas as pd
import sys

in_excel = "warnings_balkan copy.xlsx"

fgdb = arcpy.CreateFileGDB_management(out_folder_path=os.getcwd(), out_name="Workspace9.gdb").getOutput(0)

fgdb_table = arcpy.ExcelToTable_conversion(in_excel, os.path.join(fgdb, "warnings_balkan")).getOutput(0)

input_schema_dict = {}
for field in arcpy.ListFields(fgdb_table):
    input_schema_dict[field.name] = field.type

input_fields = list(input_schema_dict.keys())

poly_fc = arcpy.CreateFeatureclass_management(out_path=fgdb, out_name="warnings_balkan_poly", geometry_type="POLYGON", spatial_reference="4326").getOutput(0)

# Add needed attribute fields to the target poly FC
for field in input_schema_dict:
    if field == "OBJECTID":
        pass
    elif field == "WaText":
        print("Adding field {0}...".format(field))
        # Add field with data type from schema dict
        arcpy.AddField_management(poly_fc, str(field), input_schema_dict[field], field_length=5000)
    else:
        print("Adding field {0}...".format(field))
        # Add field with data type from schema dict
        arcpy.AddField_management(poly_fc, str(field), input_schema_dict[field])

# Helper function to create polygon object for an insert cursor
def prepare_polygon_object(poly_coords, return_objects_list=False):
    single_feature_geometry = []
    for coord_pair in poly_coords.split(" "):
        if coord_pair == '':
            pass
        else:
            # Change string to floats
            float_coord_pair = [float(coord) for coord in coord_pair.split(",")]
            single_feature_geometry.append(float_coord_pair)
    all_features_geometry = [single_feature_geometry]
    
    polygon_objects_list = []

    for feature in all_features_geometry:
        # Create a point object
        list_of_points = []
        for coordinate_pair in feature:
            y_vertex = coordinate_pair[0]
            x_vertex = coordinate_pair[1]
            point_object = arcpy.Point(x_vertex, y_vertex)
            list_of_points.append(point_object)

        # Create an arcpy array
        ap_array = arcpy.Array(list_of_points)
        # Create a polygon object
        polygon_obj = arcpy.Polygon(ap_array)
        # Append to the objects list
        polygon_objects_list.append(polygon_obj)
    if return_objects_list:
        return polygon_objects_list
    else:
        return polygon_obj

insertcursor_field_list = ["SHAPE@"] + list(input_schema_dict.keys())

# Start iterating on the target Polygonal FC with an insert cursor
with arcpy.da.InsertCursor(poly_fc, insertcursor_field_list) as insertcursor:
    
    # Start iterating on the input file geodatabase table with a searchcursor
    with arcpy.da.SearchCursor(fgdb_table, input_fields) as searchcursor:
        for row in searchcursor:
            objectid = row[0]
            waid = row[1]
            country_caption = row[2]
            area_caption = row[3]
            emma_id = row[4]
            polygon = row[5]
            walevel = row[6]
            awtcaption = row[7]
            watype = row[8]
            wafrom = row[9]
            wato = row[10]
            watext = row[11]
            print("Processing row with Object ID of {0}".format(str(objectid)))
            
            try:
                polygon_objects_list = prepare_polygon_object(polygon)
                insert_vals = (polygon_objects_list, 
                               objectid,
                               waid,
                               country_caption,
                               area_caption,
                               emma_id,
                               "NA",
                               walevel,
                               awtcaption,
                               watype,
                               wafrom,
                               wato,
                               watext)
                insertcursor.insertRow(insert_vals)
            
            except RuntimeError:
                print("An attribute could not be written!")
                print(str(RuntimeError))
                print("Attempting to write without text attribute.")
                polygon_objects_list = prepare_polygon_object(polygon)
                insert_vals = (polygon_objects_list, 
                               objectid,
                               waid,
                               country_caption,
                               area_caption,
                               emma_id,
                               "NA",
                               walevel,
                               awtcaption,
                               watype,
                               wafrom,
                               wato,
                               "Unable to write Text")
                insertcursor.insertRow(insert_vals)
            
            except ValueError:
                print("Could not read geometry from input list. Bypassing...")