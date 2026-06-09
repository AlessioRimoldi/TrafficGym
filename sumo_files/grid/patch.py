import xml.etree.ElementTree as ET

def modify_traffic_lights(input_net_path, output_net_path):
    print(f"Reading {input_net_path}...")
    tree = ET.parse(input_net_path)
    root = tree.getroot()
    
    modified_count = 0

    # Find all traffic light logic elements in the network file
    for tl in root.findall('tlLogic'):
        tl_id = tl.get('id')
        
        # Determine number of links from the first phase's state string length
        phases = tl.findall('phase')
        if not phases:
            continue
        num_links = len(phases[0].get('state'))
        
        # Clear all original phases out of this traffic light
        for phase in phases:
            tl.remove(phase)
            
        # Ensure we target the default programID "0" so it takes effect immediately
        tl.set('programID', '0')
        tl.set('type', 'static')
        tl.set('offset', '0')
        
        # Apply logic based on the number of links
        if num_links == 16:  # 4-Leg Intersections
            for i in range(4):
                # Green Phase (4 consecutive G's shifting across 16 characters)
                g_state = ['r'] * 16
                g_state[i*4 : (i+1)*4] = ['G'] * 4
                ET.SubElement(tl, "phase", duration="4", state="".join(g_state))
                
                # Yellow Phase (4 consecutive y's shifting across 16 characters)
                y_state = ['r'] * 16
                y_state[i*4 : (i+1)*4] = ['y'] * 4
                ET.SubElement(tl, "phase", duration="3", state="".join(y_state))
            modified_count += 1
            
        elif num_links == 9:  # 3-Leg Intersections
            for i in range(3):
                # Green Phase (3 consecutive G's shifting across 9 characters)
                g_state = ['r'] * 9
                g_state[i*3 : (i+1)*3] = ['G'] * 3
                ET.SubElement(tl, "phase", duration="4", state="".join(g_state))
                
                # Yellow Phase (3 consecutive y's shifting across 9 characters)
                y_state = ['r'] * 9
                y_state[i*3 : (i+1)*3] = ['y'] * 3
                ET.SubElement(tl, "phase", duration="3", state="".join(y_state))
            modified_count += 1
            
        elif num_links == 2:  # 2-Leg Intersections
            for i in range(2):
                # Green Phase (1 G shifting across 2 characters)
                g_state = ['r'] * 2
                g_state[i] = 'G'
                ET.SubElement(tl, "phase", duration="4", state="".join(g_state))
                
                # Yellow Phase (1 y shifting across 2 characters)
                y_state = ['r'] * 2
                y_state[i] = 'y'
                ET.SubElement(tl, "phase", duration="3", state="".join(y_state))
            modified_count += 1
        else:
            print(f"Skipping junction {tl_id}: Unexpected link length ({num_links})")
            # Restore the original phases if it doesn't match 16, 9, or 2
            for phase in phases:
                tl.append(phase)

    # Save the updated network map back to a file
    print(f"Writing updated network to {output_net_path}...")
    tree.write(output_net_path, encoding="utf-8", xml_declaration=True)
    print(f"Success! Updated custom phase logic for {modified_count} traffic lights.")

if __name__ == "__main__":
    # Make sure 'net.net.xml' is in the same folder as this script, or update the path here
    modify_traffic_lights('net.net.xml', 'net_updated.net.xml')
