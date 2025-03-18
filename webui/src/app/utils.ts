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

/**
 * Truncates text to a specific length and adds an ellipsis
 */
export const truncateText = (text: string, maxLength: number): string => {
  if (!text || text.length <= maxLength) return text;
  return `${text.substring(0, maxLength)}...`;
};

/**
 * Format a timestamp to a human-readable date
 */
export const formatTimestamp = (timestamp: number): string => {
  return new Date(timestamp * 1000).toLocaleString();
};

/**
 * Format bytes into a human-readable format
 */
export const formatBytes = (bytes: number, decimals: number = 2): string => {
  if (bytes === 0) return '0 Bytes';
  
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
  
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  
  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
};

/**
 * Calculate uptime from creation timestamp
 */
export const calculateUptime = (timestamp: number): string => {
  const now = Math.floor(Date.now() / 1000);
  const uptimeSeconds = now - timestamp;
  
  if (uptimeSeconds < 60) return `${uptimeSeconds} seconds`;
  if (uptimeSeconds < 3600) return `${Math.floor(uptimeSeconds / 60)} minutes`;
  if (uptimeSeconds < 86400) return `${Math.floor(uptimeSeconds / 3600)} hours`;
  return `${Math.floor(uptimeSeconds / 86400)} days`;
};

/**
 * Format slot ranges for better display
 */
export const formatSlotRanges = (ranges: string[]): string => {
  if (!ranges || ranges.length === 0) return "None";
  if (ranges.length <= 2) return ranges.join(", ");
  return `${ranges[0]}, ${ranges[1]}, ... (+${ranges.length - 2} more)`;
};
