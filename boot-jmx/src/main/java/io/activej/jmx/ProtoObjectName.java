/*
 * Copyright (C) 2020 ActiveJ LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activej.jmx;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

public final class ProtoObjectName {
	@NotNull
	private final String className;
	@NotNull
	private final String packageName;
	@Nullable
	private final Object qualifier;
	@Nullable
	private final String scope;
	@Nullable
	private final String workerPoolQualifier;
	@Nullable
	private final String workerId;
	@Nullable
	private final List<String> genericParameters;

	public ProtoObjectName(@NotNull String className, @NotNull String packageName, @Nullable Object qualifier,
			@Nullable String scope, @Nullable String workerPoolQualifier, @Nullable String workerId,
			@Nullable List<String> genericParameters) {
		this.className = className;
		this.packageName = packageName;
		this.qualifier = qualifier;
		this.scope = scope;
		this.workerPoolQualifier = workerPoolQualifier;
		this.workerId = workerId;
		this.genericParameters = genericParameters;
	}

	public static ProtoObjectName create(@NotNull String className, @NotNull String packageName) {
		return new ProtoObjectName(className, packageName, null, null, null, null, null);
	}

	public ProtoObjectName withClassName(@NotNull String className) {
		return new ProtoObjectName(className, packageName, qualifier, scope, workerPoolQualifier, workerId, genericParameters);
	}

	public ProtoObjectName withPackageName(@NotNull String packageName) {
		return new ProtoObjectName(className, packageName, qualifier, scope, workerPoolQualifier, workerId, genericParameters);
	}

	public ProtoObjectName withQualifier(@Nullable Object qualifier) {
		return new ProtoObjectName(className, packageName, qualifier, scope, workerPoolQualifier, workerId, genericParameters);
	}

	public ProtoObjectName withScope(@Nullable String scope) {
		return new ProtoObjectName(className, packageName, qualifier, scope, workerPoolQualifier, workerId, genericParameters);
	}

	public ProtoObjectName withWorkerPoolQualifier(@Nullable String workerPoolQualifier) {
		return new ProtoObjectName(className, packageName, qualifier, scope, workerPoolQualifier, workerId, genericParameters);
	}

	public ProtoObjectName withWorkerId(@Nullable String workerId) {
		return new ProtoObjectName(className, packageName, qualifier, scope, workerPoolQualifier, workerId, genericParameters);
	}

	public ProtoObjectName withGenericParameters(@Nullable List<String> genericParameters) {
		ArrayList<String> list = genericParameters == null ? null : new ArrayList<>(genericParameters);
		return new ProtoObjectName(className, packageName, qualifier, scope, workerPoolQualifier, workerId, list);
	}

	// region getters
	public @NotNull String getClassName() {
		return className;
	}

	public @NotNull String getPackageName() {
		return packageName;
	}

	public @Nullable Object getQualifier() {
		return qualifier;
	}

	public @Nullable String getScope() {
		return scope;
	}

	public @Nullable String getWorkerPoolQualifier() {
		return workerPoolQualifier;
	}

	public @Nullable List<String> getGenericParameters() {
		return genericParameters;
	}

	public @Nullable String getWorkerId() {
		return workerId;
	}
	// endregion

	@Override
	public String toString() {
		return "ProtoObjectName{" +
				"className='" + className + '\'' +
				", packageName='" + packageName + '\'' +
				(qualifier == null ? "" : (", qualifier='" + qualifier + '\'')) +
				(scope == null ? "" : (", scope='" + scope + '\'')) +
				(workerPoolQualifier == null ? "" : (", workerPoolQualifier='" + workerPoolQualifier + '\'')) +
				(workerId == null ? "" : (", workerId='" + workerId + '\'')) +
				(genericParameters == null ? "" : (", genericParameters=" + genericParameters)) +
				'}';
	}
}
