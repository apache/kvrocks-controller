/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License. 
 */

import { Button, Divider, Drawer, List, ListItem, ListItemButton, ListItemIcon, ListItemText } from "@mui/material";
import InboxIcon from '@mui/icons-material/MoveToInbox';
import MailIcon from '@mui/icons-material/Mail';
import { fetchNamespaces } from "@/app/lib/api";

const dividerColor = 'rgba(255,255,255,.2)';
export default async function Sidebar() {
    const namespaces = await fetchNamespaces();
    return (
        <div className="w-60 h-full flex">
            <List className="w-full overflow-y-auto">
                <div className="mt-2 mb-4 text-center">
                    <Button variant="outlined">Create Namespace</Button>
                </div>
                {namespaces.map((text, index) => (<>
                    {index === 0 && (
                        <Divider variant="middle" sx={{ bgcolor: dividerColor}}/>
                    )}
                    <ListItem key={text} disablePadding>
                        <ListItemButton>
                            <ListItemText classes={{primary: 'overflow-hidden text-ellipsis text-nowrap'}} primary={text} />
                        </ListItemButton>
                    </ListItem>
                    <Divider variant="middle" sx={{ bgcolor: dividerColor}}/>
                </>))}
            </List>
            <Divider orientation="vertical" flexItem sx={{bgcolor: dividerColor}}/>
        </div>
    )
}