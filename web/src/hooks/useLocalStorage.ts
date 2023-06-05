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
import { useState } from 'react';

export function useLocalstorage<T extends number | string | object>(key: string, initValue: T): [value: T, setValue: (value: T) => any] {
    function transformToString(value: T): string {
        let valueStr = '';
        if (typeof value == 'string') {
            valueStr = value;
        } else if (typeof value == 'number') {
            valueStr = `${value}`;
        } else if (typeof value == 'object') {
            valueStr = JSON.stringify(value);
        }
        return valueStr;
    }
    function transformToT(value: string): T {
        try {
            if (typeof initValue == 'string') {
                return value as T;
            } else if (typeof initValue == 'number') {
                return parseInt(value) as T;
            } else if (typeof initValue == 'object') {
                return JSON.parse(value);
            }
        } catch (error) {
            console.error(error);
        }
        return value as T;
    }
    const [value, setValue] = useState<T>(() => {
        const storedValue: T = transformToT(localStorage.getItem(key) || '');
        if(!storedValue) {
            localStorage.setItem(key, transformToString(initValue));
        }
        return storedValue || initValue;
    });
    const setLocalstorageValue = (newValue: T) => {
        setValue(newValue);
        localStorage.setItem(key, transformToString(newValue));
    };
    return [value, setLocalstorageValue];
}