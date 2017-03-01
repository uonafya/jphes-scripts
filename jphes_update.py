import psycopg2
failed_jphes_ipsl_members = []
# jphes_ipsl_members = {}

try:
    conn = psycopg2.connect("dbname='jphes' user='username' host='localhost' password='password'")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM jphes_ipsl_members")
    rows = cursor.fetchall()
    if len(rows) > 0:
        column_names = [desc[0] for desc in cursor.description]
        for row in rows:
            jphes_ipsl_members = dict(zip(column_names, row))
            print jphes_ipsl_members
            if jphes_ipsl_members:
                # Get organisation unit id
                cursor.execute("SELECT * from organisationunit WHERE code = '%s'" % jphes_ipsl_members['mflcode'])
                org_unit_row = cursor.fetchone()
                if not org_unit_row:
                    print 'ORGANISATION UNIT not available for jphes_ipsl_member of datimcode %s therefore skipped' % jphes_ipsl_members['datimcode']
                    failed_jphes_ipsl_members.append(jphes_ipsl_members['datimcode'])
                    continue
                org_unit_column_names = [desc[0] for desc in cursor.description]
                org_unit_data = dict(zip(org_unit_column_names, org_unit_row))
                print org_unit_data

                # Get Category Options Combo
                cursor.execute("SELECT categoryoptioncombo.categoryoptioncomboid, categoryoptioncombo.name "
                               "FROM jphes_mechanismunit, categoryoptioncombo WHERE jphes_mechanismunit.code = '%s' "
                               "AND categoryoptioncombo.name = jphes_mechanismunit.name" % jphes_ipsl_members['datimcode'])
                combo_row = cursor.fetchone()
                if len(combo_row) > 0:
                    combo_column_names = [desc[0] for desc in cursor.description]
                    combo_data = dict(zip(combo_column_names, combo_row))
                    print combo_data
                else:
                    print 'No CATEGORY OPTIONS COMBO for jphes_ipsl_member datimcode: ', jphes_ipsl_members['datimcode']
                cursor.execute(
                    "SELECT * FROM jphes_programdataelements WHERE jphes_programdataelements.programid = '%s'" %
                    jphes_ipsl_members['programid'])
                program_data_elements = cursor.fetchall()
                if len(program_data_elements) > 0 and combo_data:
                    pde_column_names = [desc[0] for desc in cursor.description]
                    for pde in program_data_elements:
                        pde_data = dict(zip(pde_column_names, pde))
                        print pde_data
                        cursor.execute("UPDATE datavalue set attributeoptioncomboid = %s WHERE sourceid = %s AND dataelementid = %s AND "
                                       "datavalue.periodid = %s" % (combo_data['categoryoptioncomboid'], org_unit_data['organisationunitid'],
                                       pde_data['dataelementid'], jphes_ipsl_members['periodid']))
                        conn.commit()
                        print 'Data Updated'
                        # data_values = cursor.fetchall()
                        # if data_values:
                        #     data_values_column_names = [desc[0] for desc in cursor.description]
                        #     for data_value in data_values:
                        #         data_value_data = dict(zip(data_values_column_names, data_value))
                        #         print data_value_data
                        # else:
                        #     print 'No DATA VALUE for jphes_ipsl_member datimcode: ', jphes_ipsl_members['datimcode']
                else:
                    failed_jphes_ipsl_members.append(jphes_ipsl_members['datimcode'])
                    print 'No PROGRAM DATA ELEMENTS for jphes_ipsl_member datimcode: ', jphes_ipsl_members['datimcode']

    print 'Failed jphes_ipsl_members: ', failed_jphes_ipsl_members
except Exception, e:
    print 'Error: ', e
    print "I am unable to connect to the database JPHES"
