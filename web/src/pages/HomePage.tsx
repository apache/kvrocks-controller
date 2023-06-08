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
 *
 */

import { Button } from 'antd';
import { useContext } from 'react';
import { NamespaceCreationDialogDisplayContext } from '../App';

export function HomePage() {
    const openDialog = useContext(NamespaceCreationDialogDisplayContext);
    return (<div style={{height: '100%'}} className="centered-horizontally-and-vertically-parent">
        <Button
            className='centered-horizontally-and-vertically'
            size='large'
            type='primary'
            onClick={openDialog}
        >Create Namespace</Button>
    </div>);
}