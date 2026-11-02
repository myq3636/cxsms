package com.king.message.gmms;

import java.lang.reflect.Field;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

/** Scratch check of MessageStoreManager.withInlineDrDispatch (not part of the repo). */
public class InlineDrCheck {
	static int failures = 0;

	static void check(boolean ok, String what) {
		System.out.println((ok ? "PASS " : "FAIL ") + what);
		if (!ok) failures++;
	}

	@SuppressWarnings("unchecked")
	static boolean inline() throws Exception {
		Field f = MessageStoreManager.class.getDeclaredField("inlineDrDispatch");
		f.setAccessible(true);
		return ((ThreadLocal<Boolean>) f.get(null)).get() != null;
	}

	public static void main(String[] args) throws Exception {
		check(!inline(), "off by default");
		boolean inside = MessageStoreManager.withInlineDrDispatch(new Callable<Boolean>() {
			public Boolean call() throws Exception {
				return inline();
			}
		});
		check(inside, "on inside withInlineDrDispatch");
		check(!inline(), "off again after it returns");

		final boolean[] nested = new boolean[2];
		MessageStoreManager.withInlineDrDispatch(new Callable<Void>() {
			public Void call() throws Exception {
				MessageStoreManager.withInlineDrDispatch(new Callable<Void>() {
					public Void call() throws Exception {
						nested[0] = inline();
						return null;
					}
				});
				nested[1] = inline();
				return null;
			}
		});
		check(nested[0] && nested[1], "nested call keeps it on until the outermost returns");
		check(!inline(), "off after nested calls");

		try {
			MessageStoreManager.withInlineDrDispatch(new Callable<Void>() {
				public Void call() {
					throw new IllegalStateException("boom");
				}
			});
		} catch (IllegalStateException expected) {
		}
		check(!inline(), "off after the task throws");

		final AtomicReference<Boolean> other = new AtomicReference<Boolean>();
		MessageStoreManager.withInlineDrDispatch(new Callable<Void>() {
			public Void call() throws Exception {
				Thread t = new Thread() {
					public void run() {
						try {
							other.set(inline());
						} catch (Exception e) {
							other.set(Boolean.TRUE);
						}
					}
				};
				t.start();
				t.join();
				return null;
			}
		});
		check(Boolean.FALSE.equals(other.get()), "other threads keep using the pool");

		System.out.println(failures == 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED");
		System.exit(failures == 0 ? 0 : 1);
	}
}
